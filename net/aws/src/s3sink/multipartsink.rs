// Copyright (C) 2019 Amazon.com, Inc. or its affiliates <mkolny@amazon.com>
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// <https://mozilla.org/MPL/2.0/>.
//
// SPDX-License-Identifier: MPL-2.0

use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;
use gst_base::prelude::*;
use gst_base::subclass::prelude::*;

use aws_sdk_s3::{
    config::{self, retry::RetryConfig, Credentials, Region},
    error::ProvideErrorMetadata,
    operation::{
        abort_multipart_upload::builders::AbortMultipartUploadFluentBuilder,
        complete_multipart_upload::builders::CompleteMultipartUploadFluentBuilder,
        create_multipart_upload::builders::CreateMultipartUploadFluentBuilder,
        upload_part::builders::UploadPartFluentBuilder,
    },
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};

use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::convert::From;
use std::sync::Mutex;
use std::time::Duration;

use crate::s3url::*;
use crate::s3utils::{self, duration_from_millis, duration_to_millis, WaitError};

use super::OnError;

const DEFAULT_FORCE_PATH_STYLE: bool = false;
const DEFAULT_RETRY_ATTEMPTS: u32 = 5;
const DEFAULT_NUM_CACHED_PARTS: i64 = 0;
const DEFAULT_MULTIPART_UPLOAD_ON_ERROR: OnError = OnError::DoNothing;

// General setting for create / abort requests
const DEFAULT_REQUEST_TIMEOUT_MSEC: u64 = 15_000;
const DEFAULT_RETRY_DURATION_MSEC: u64 = 60_000;
// This needs to be independently configurable, as the part size can be upto 5GB
const DEFAULT_UPLOAD_PART_REQUEST_TIMEOUT_MSEC: u64 = 10_000;
const DEFAULT_UPLOAD_PART_RETRY_DURATION_MSEC: u64 = 60_000;
// CompletedMultipartUpload can take minutes to complete, so we need a longer value here
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
const DEFAULT_COMPLETE_REQUEST_TIMEOUT_MSEC: u64 = 600_000; // 10 minutes
const DEFAULT_COMPLETE_RETRY_DURATION_MSEC: u64 = 3_600_000; // 60 minutes

// https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
const MAX_MULTIPART_NUMBER: i64 = 10000;

struct Started {
    client: Client,
    // the active part's buffer
    buffer: Vec<u8>,
    // active buffer's "data size" represents the last offset of
    // data that was written to the buffer prior to manually setting
    // the buffer's len() during a seek operation.
    buffer_data_size: usize,
    upload_id: String,
    part_number: i64, // the active part number
    completed_parts: Vec<CompletedPart>,
    cache: UploaderPartCache,
    // The overall upload's current write head position.
    // Given the AWS limits of 5GB part size and 10k parts, this is:
    //   (5*1024^3) * 10,000 = 53,687,091,200,000, a 64-bit number.
    upload_pos: u64,
}

impl Started {
    pub fn new(
        client: Client,
        buffer: Vec<u8>,
        upload_id: String,
        num_cache_parts: i64,
    ) -> Started {
        Started {
            client,
            buffer,
            buffer_data_size: 0,
            upload_id,
            part_number: 1,
            completed_parts: Vec::new(),
            cache: UploaderPartCache::new(num_cache_parts),
            upload_pos: 0,
        }
    }

    pub fn increment_part_number(&mut self) -> Result<i64, gst::ErrorMessage> {
        if self.part_number > MAX_MULTIPART_NUMBER {
            return Err(gst::error_msg!(
                gst::ResourceError::Failed,
                [
                    "Maximum number of parts ({}) reached.",
                    MAX_MULTIPART_NUMBER
                ]
            ));
        }

        self.part_number += 1;
        Ok(self.part_number)
    }
}

struct PartInfo {
    pub buffer: Vec<u8>,
    pub data_size: usize,
}

impl PartInfo {
    fn new(size: usize, capacity: usize) -> PartInfo {
        PartInfo {
            buffer: Vec::with_capacity(capacity),
            data_size: size,
        }
    }
}

struct UploaderPartCache {
    from_head: bool,
    max_depth: usize,
    cache: Vec<PartInfo>,
}

impl UploaderPartCache {
    pub fn new(depth: i64) -> UploaderPartCache {
        UploaderPartCache {
            from_head: (depth > 0),
            max_depth: depth.abs().try_into().unwrap(),
            cache: Default::default(),
        }
    }

    pub fn get<T: Into<usize>>(&self, part_num: T) -> Option<&PartInfo> {
        let part_num = part_num.into() as usize;
        self.cache.get(part_num - 1)
    }

    #[allow(unused)]
    pub fn get_mut<T: Into<usize>>(&mut self, part_num: T) -> Option<&mut PartInfo> {
        let part_num = part_num.into() as usize;
        self.cache.get_mut(part_num - 1)
    }

    /**
     * Returns the beginning and ending offsets (in bytes) that can be retrieved
     * from the cache.  This might be a helpful alternative to calling find if
     * the only interest is if one can expect the find to succeed.
     */
    pub fn coverage_limits(&self) -> (u64, u64) {
        let mut beginning: Option<u64> = None;
        let mut ending: Option<u64> = None;
        let mut offset: u64 = 0;

        for record in self.cache.iter() {
            let offset_after = offset + record.buffer.len() as u64;
            if record.buffer.len() > 0 {
                if beginning.is_none() {
                    beginning = Some(offset);
                }

                if beginning.is_some() {
                    ending = Some(offset_after - 1);
                }

                offset = offset_after;
            } else if record.data_size > 0 {
                offset += record.data_size as u64;
            }
        }
        (beginning.unwrap_or(0), ending.unwrap_or(0))
    }

    pub fn coverage_range(&self) -> std::ops::Range<u64> {
        let (start, end) = self.coverage_limits();
        start..end
    }

    pub fn update_or_append<T: Into<usize>>(&mut self, part_num: T, buffer: &Vec<u8>) -> bool {
        // confirm the part number makes logical sense: positive, non-zero, less than
        // the maximum amount permitted by AWS.
        let part_num = part_num.into() as usize;
        let max_part: usize = MAX_MULTIPART_NUMBER.try_into().unwrap();
        let part_idx = part_num - 1;
        if part_num > max_part || part_num == 0 || part_idx > self.cache.len() {
            return false;
        }

        if part_idx == self.cache.len() {
            // Insert new one
            // NOTE: will deal with buffer later if necessary
            self.cache
                .push(PartInfo::new(buffer.len(), buffer.capacity()));
        } else {
            // update data size, at a minimum
            self.cache[part_idx].data_size = buffer.len();
        }

        if self.max_depth > 0 {
            let first: usize;
            let last: usize;

            if self.from_head {
                // Keeping up to the first N buffers
                last = self.max_depth - 1;
                first = 0;
            } else {
                // Keeping the last / most recent N buffers
                last = self.cache.len();
                if self.max_depth < last {
                    let l = last as isize;
                    let d = self.max_depth as isize;
                    first = (l - d).try_into().unwrap();
                } else {
                    first = 0;
                }
            }

            for (i, part) in self.cache.iter_mut().enumerate() {
                if first <= i && i <= last {
                    // part is in 'retain' range
                    if i == part_idx {
                        // 'buffer' is this part; update it
                        part.buffer = buffer.to_owned();
                        part.data_size = buffer.len();
                    }
                } else {
                    // part not in 'retain' range; ensure it's cleared.
                    part.buffer.clear();
                }
            }
        }
        return true;
    }

    #[allow(unused)]
    pub fn get_copy<T: Into<usize>>(
        &self,
        part_num: T,
        buffer: &mut Vec<u8>,
        size: &mut usize,
    ) -> bool {
        match self.get(part_num) {
            Some(part) => {
                *buffer = part.buffer.to_vec();
                *size = part.data_size;
                true
            }
            None => false,
        }
    }

    /**
     * Find 'offset' within the part cache (or don't).  If found, 'part_num' and 'size' will
     * be from the cache.  If the buffer was stored per the configuration, then 'buffer' will
     * be filled with a copy.
     */
    pub fn find(&self, offset: u64, part_num: &mut u16) -> Result<&PartInfo, gst::ErrorMessage> {
        let mut i = 1;
        let mut start = 0_u64;

        for item in self.cache.iter() {
            let item_size: u64 = item.data_size.try_into().unwrap();
            let range = start..start + item_size;

            if range.contains(&offset) {
                *part_num = i;
                return Ok(self.get(i).unwrap());
            }
            i += 1;
            start += item_size;
        }
        return Err(gst::error_msg!(
            gst::ResourceError::NotFound,
            ["Could not find part {i} in cache"]
        ));
    }
}

// Tests for UploaderPartCache
#[cfg(test)]
mod tests {
    use crate::s3sink::multipartsink::UploaderPartCache;

    /**
     * Test inserts a buffer of length 100
     */
    #[test]
    fn cache_disabled() {
        const DEPTH: i64 = 0;
        const SIZE_BUFFER: usize = 100;

        let mut uut = UploaderPartCache::new(DEPTH);
        let buffer = vec![0; SIZE_BUFFER];
        let mut part_num: u16 = 0;

        // Nothing stored, so cache availability.
        let mut limits = uut.coverage_limits();
        assert_eq!(0, limits.0);
        assert_eq!(0, limits.1);

        // Insert and 'find' should both be TRUE since we're looking for the
        // cached buffer that would have offset 50 in it.  The resulting
        // buffer however is empty, since caching is "disabled".
        assert_eq!(uut.cache.len(), 0);
        assert!(uut.update_or_append(1_usize, &buffer));
        assert_eq!(uut.cache.len(), 1);

        let result = uut.find(SIZE_BUFFER as u64 / 2, &mut part_num).unwrap();
        assert_eq!(0, result.buffer.len());
        assert_eq!(SIZE_BUFFER, result.buffer.capacity());
        assert_eq!(1, part_num);

        // 'get' should work too, same behavior as above since caching
        // of the contents of the part is disabled.
        let mut out_buffer = vec![0; SIZE_BUFFER];
        let mut out_buffer_size: usize = 0;
        assert!(uut.get_copy(part_num, &mut out_buffer, &mut out_buffer_size));
        assert_eq!(out_buffer.len(), 0);
        assert_eq!(SIZE_BUFFER, out_buffer_size);

        // Still nothing actually stored in the cache, so still 0's for
        // the limits.
        limits = uut.coverage_limits();
        assert_eq!(0, limits.0);
        assert_eq!(0, limits.1);
    }

    /**
     * Push 3 100 byte parts and verify 'find' gets
     * the right parts vs. the offsets.
     *    Part 1:   0 -  99
     *    Part 2: 100 - 199
     *    Part 3: 200 - 299
     */
    #[test]
    fn find_by_offset() {
        const BUFFER_SIZE: usize = 100;
        const NUM_PARTS: usize = 3;
        let mut uut = UploaderPartCache::new(0);

        // Populate the cache
        for i in 1..NUM_PARTS as usize {
            assert_eq!(uut.cache.len(), i - 1);
            assert!(uut.update_or_append(i, &vec![0; BUFFER_SIZE]));
            assert_eq!(uut.cache.len(), i);

            // coverage offsets should be unchanged; depth is 0 (no cache).
            let limits = uut.coverage_limits();
            assert_eq!(0, limits.0);
            assert_eq!(0, limits.1);
        }

        // Validate the cache offsets
        let mut offset_start: u64 = 0;
        for i in 1..NUM_PARTS as u16 {
            let mut out_part_num = 0;
            let offset_end = (offset_start + BUFFER_SIZE as u64) - 1;

            let mut result = uut.find(offset_start, &mut out_part_num);
            assert!(result.is_ok());
            assert_eq!(out_part_num, i);

            result = uut.find(offset_end, &mut out_part_num);
            assert!(result.is_ok());
            assert_eq!(out_part_num, i);

            offset_start = offset_end + 1;
        }
    }

    /**
     * Test various failure modes for cache misses on insert/update and get
     */
    #[test]
    fn cache_miss() {
        const BUFFER_SIZE: usize = 100;
        let mut uut = UploaderPartCache::new(0);
        let mut out_buffer_size = 0;
        let mut out_buffer: Vec<u8> = Default::default();
        let mut out_part_num = 0;
        let buffer = vec![0; BUFFER_SIZE];

        // Should not be able to find anything; nothing exists yet.
        assert!(uut.find(20, &mut out_part_num).is_err());

        // Should not be able to get the first part, it hasn't been inserted.
        assert!(!uut.get_copy(1_u16, &mut out_buffer, &mut out_buffer_size));

        // Size is 0, so inserting part number 2 is invalid; this should fail.
        assert!(!uut.update_or_append(2_usize, &out_buffer));

        // Should be able to insert part 1
        assert!(uut.update_or_append(1_usize, &buffer));

        // Should be able to access part 1, but it's buffer should be empty
        // since it's beyond the depth being retained in the cache.
        let result = uut.find(BUFFER_SIZE as u64 - 1, &mut out_part_num).unwrap();
        assert_eq!(result.buffer.len(), 0);
        assert_eq!(result.data_size, BUFFER_SIZE);

        // Should not be able to find offset 100 since that would be part 2
        assert!(uut.find(100, &mut out_part_num).is_err());
    }

    /**
     * Verify the behavior of retaining the first N parts, remainders are empty.
     */
    #[test]
    fn retain_head() {
        const BUFFER_SIZE: usize = 100;
        let mut uut = UploaderPartCache::new(2);
        let in_buffer = vec![0; BUFFER_SIZE];
        let mut out_buffer: Vec<u8> = Default::default();
        let mut out_buffer_size: usize = 0;

        assert_eq!(uut.cache.len(), 0);

        for i in 1..=uut.max_depth + 1 {
            let mut temp: Vec<u8> = Default::default();
            let mut temp_size = 0 as usize;

            assert!(uut.update_or_append(i, &in_buffer));

            // Since this is head retention, immediately upon insertion
            // if the part number is within the limit, it should be kept,
            // otherwise immediately dropped.
            assert!(uut.get_copy(i, &mut temp, &mut temp_size));
            if i <= uut.max_depth {
                // Retained
                assert!(temp.len() != 0);
                assert!(temp_size == BUFFER_SIZE);
            } else {
                // Dropped
                assert!(temp.len() == 0);
                assert!(temp_size == BUFFER_SIZE);
            }
        }
        // There should be 3 parts in the cache (though only 2 are retained).
        assert_eq!(uut.cache.len(), 3);

        // Coverage offsets should be 0 to BUFFER_SIZE*2 - 1 (the end of
        // buffer 2).
        let offsets = uut.coverage_limits();
        assert_eq!(0, offsets.0);
        assert_eq!((BUFFER_SIZE as u64) * 2 - 1, offsets.1);

        // 1 and 2 should have a buffer, 3 should not.
        assert!(uut.get_copy(1_u16, &mut out_buffer, &mut out_buffer_size));
        assert!(out_buffer.len() == BUFFER_SIZE);
        assert!(out_buffer_size == BUFFER_SIZE);
        out_buffer.clear();
        out_buffer_size = 0;

        assert!(uut.get_copy(2_u16, &mut out_buffer, &mut out_buffer_size));
        assert!(out_buffer.len() == BUFFER_SIZE);
        assert!(out_buffer_size == BUFFER_SIZE);
        out_buffer.clear();
        out_buffer_size = 0;

        assert!(uut.get_copy(3_u16, &mut out_buffer, &mut out_buffer_size));
        assert!(out_buffer.len() == 0);
        assert!(out_buffer_size == BUFFER_SIZE);
    }

    /**
     * Verify the behavior of retaining the last N parts, remainders are empty.
     */
    #[test]
    fn retain_tail() {
        const BUFFER_SIZE: usize = 100;
        let mut uut = UploaderPartCache::new(-2);
        let in_buffer = vec![0; BUFFER_SIZE];
        let mut out_buffer: Vec<u8> = Default::default();
        let mut out_buffer_size = 0;

        assert_eq!(uut.cache.len(), 0);
        for i in 1..=uut.max_depth + 1 {
            let mut temp: Vec<u8> = Default::default();
            let mut temp_size = 0 as usize;

            assert!(uut.update_or_append(i, &in_buffer));

            // Since this is tail retention, the most recent part should
            // always be retained.
            assert!(uut.get_copy(i, &mut temp, &mut temp_size));
            assert!(temp.len() != 0);
            assert!(temp_size == BUFFER_SIZE);
        }

        // 1 should not have a buffer, 2 and 3 should.
        assert!(uut.get_copy(1_usize, &mut out_buffer, &mut out_buffer_size));
        assert!(out_buffer.len() == 0);
        assert!(out_buffer_size == BUFFER_SIZE);
        out_buffer.clear();
        out_buffer_size = 0;

        assert!(uut.get_copy(2_usize, &mut out_buffer, &mut out_buffer_size));
        assert!(out_buffer.len() == BUFFER_SIZE);
        assert!(out_buffer_size == BUFFER_SIZE);
        out_buffer.clear();
        out_buffer_size = 0;

        assert!(uut.get_copy(3_usize, &mut out_buffer, &mut out_buffer_size));
        assert!(out_buffer.len() == BUFFER_SIZE);
        assert!(out_buffer_size == BUFFER_SIZE);

        // Coverage offsets should be BUFFER_SIZE to BUFFER_SIZE*3 - 1 (the end of
        // buffer 2).
        let offsets = uut.coverage_limits();
        assert_eq!(BUFFER_SIZE as u64, offsets.0);
        assert_eq!((BUFFER_SIZE as u64) * 3 - 1, offsets.1);
    }
}

#[derive(Default)]
enum State {
    #[default]
    Stopped,
    Completed,
    Started(Started),
}

struct Settings {
    region: Region,
    bucket: Option<String>,
    key: Option<String>,
    content_type: Option<String>,
    content_disposition: Option<String>,
    buffer_size: usize,
    num_cached_parts: i64,
    access_key: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    metadata: Option<gst::Structure>,
    retry_attempts: u32,
    multipart_upload_on_error: OnError,
    request_timeout: Duration,
    endpoint_uri: Option<String>,
    force_path_style: bool,
}

impl Settings {
    fn to_uri(&self) -> String {
        GstS3Url {
            region: self.region.clone(),
            bucket: self.bucket.clone().unwrap(),
            object: self.key.clone().unwrap(),
            version: None,
        }
        .to_string()
    }

    fn to_metadata(&self, imp: &S3Sink) -> Option<HashMap<String, String>> {
        self.metadata.as_ref().map(|structure| {
            let mut hash = HashMap::new();

            for (key, value) in structure.iter() {
                if let Ok(Ok(value_str)) = value.transform::<String>().map(|v| v.get()) {
                    gst::log!(CAT, imp: imp, "metadata '{}' -> '{}'", key, value_str);
                    hash.insert(key.to_string(), value_str);
                } else {
                    gst::warning!(
                        CAT,
                        imp: imp,
                        "Failed to convert metadata '{}' to string ('{:?}')",
                        key,
                        value
                    );
                }
            }

            hash
        })
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            region: Region::new("us-west-2"),
            bucket: None,
            key: None,
            content_type: None,
            content_disposition: None,
            access_key: None,
            secret_access_key: None,
            session_token: None,
            metadata: None,
            buffer_size: 0,
            num_cached_parts: DEFAULT_NUM_CACHED_PARTS,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
            multipart_upload_on_error: DEFAULT_MULTIPART_UPLOAD_ON_ERROR,
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MSEC),
            endpoint_uri: None,
            force_path_style: DEFAULT_FORCE_PATH_STYLE,
        }
    }
}

#[derive(Default)]
pub struct S3Sink {
    url: Mutex<Option<GstS3Url>>,
    settings: Mutex<Settings>,
    state: Mutex<State>,
    canceller: Mutex<s3utils::Canceller>,
    abort_multipart_canceller: Mutex<s3utils::Canceller>,
    eos_pending: Mutex<bool>,
    write_will_cache_miss: Mutex<bool>,
}

static CAT: Lazy<gst::DebugCategory> = Lazy::new(|| {
    gst::DebugCategory::new(
        "awss3sink",
        gst::DebugColorFlags::empty(),
        Some("Amazon S3 Sink"),
    )
});

impl S3Sink {
    fn flush_multipart_upload(&self, state: &mut Started) {
        let settings = self.settings.lock().unwrap();
        match settings.multipart_upload_on_error {
            OnError::Abort => {
                gst::log!(
                    CAT,
                    imp: self,
                    "Aborting multipart upload request with id: {}",
                    state.upload_id
                );
                match self.abort_multipart_upload_request(state) {
                    Ok(()) => {
                        gst::log!(
                            CAT,
                            imp: self,
                            "Aborting multipart upload request succeeded."
                        );
                    }
                    Err(err) => gst::error!(
                        CAT,
                        imp: self,
                        "Aborting multipart upload failed: {}",
                        err.to_string()
                    ),
                }
            }
            OnError::Complete => {
                gst::log!(
                    CAT,
                    imp: self,
                    "Completing multipart upload request with id: {}",
                    state.upload_id
                );
                match self.complete_multipart_upload_request(state) {
                    Ok(()) => {
                        gst::log!(
                            CAT,
                            imp: self,
                            "Complete multipart upload request succeeded."
                        );
                    }
                    Err(err) => gst::error!(
                        CAT,
                        imp: self,
                        "Completing multipart upload failed: {}",
                        err.to_string()
                    ),
                }
            }
            OnError::DoNothing => (),
        }
    }

    /**
     * Creates a part upload request using the current buffer (clearing it)
     * and then sending the request (thus uploading the buffer).  Upon
     * successful completion, the completed part information is added to
     * the state.
     */
    fn flush_current_buffer(&self) -> Result<(), Option<gst::ErrorMessage>> {
        let upload_part_req = self.create_upload_part_request()?;

        if upload_part_req.is_none() {
            return Ok(()); // nothing to do here.
        }

        let mut state = self.state.lock().unwrap();
        let state = match *state {
            State::Started(ref mut started_state) => started_state,
            State::Completed => {
                unreachable!("Upload should not be completed yet");
            }
            State::Stopped => {
                unreachable!("Element should be started");
            }
        };

        let upload_part_req_future = upload_part_req.unwrap().send();
        let output =
            s3utils::wait(&self.canceller, upload_part_req_future).map_err(|err| match err {
                WaitError::FutureError(err) => {
                    self.flush_multipart_upload(state);
                    Some(gst::error_msg!(
                        gst::ResourceError::OpenWrite,
                        ["Failed to upload part: {err}: {}", err.meta()]
                    ))
                }
                WaitError::Cancelled => None,
            })?;

        let completed_part = CompletedPart::builder()
            .set_e_tag(output.e_tag)
            .set_part_number(Some(state.part_number as i32))
            .build();

        // Update or replace the completed part
        match state
            .completed_parts
            .get_mut((state.part_number - 1) as usize)
        {
            Some(part) => *part = completed_part,
            None => state.completed_parts.push(completed_part),
        }

        // state.buffer_data_size is still set to the length of this most recently uploaded
        // buffer, so increment the global write head position (upload_pos) with it.
        state.upload_pos += state.buffer_data_size as u64;

        gst::info!(CAT, imp: self, "Uploaded part {}", state.part_number);

        // Increment part number
        state.increment_part_number()?;

        // There are a few cases to check:
        // 1. OK: N/A -> "new" part that will append the upload
        // 2. OK: cache -> cached part
        // 3. ERR: cache -> "new" part that will NOT append the upload (cache miss)
        //
        // If the cache.get() returns a record:
        //   1. This current part number will NOT be appending the upload.
        //   2. If the buffer is empty, it is a cache miss.
        // Check if this part should come from cache
        let eos_pending = self.eos_pending.lock().unwrap();
        match state.cache.get(state.part_number as usize) {
            Some(part) => {
                if part.buffer.is_empty() {
                    if false == *eos_pending {
                        // Cache miss (case 3) -- the part number was known but the buffer
                        // was not stored in the cache (because of the cache configuration).
                        *self.write_will_cache_miss.lock().unwrap() = true;
                        state.buffer_data_size = 0;
                        gst::debug!(CAT, imp:self, "Next write will cause a cache miss unless another seek is performed.");
                    }
                } else {
                    // Cache hit (case 2) -- the part number was known and the buffer
                    // was a part of the cache
                    state.buffer = part.buffer.to_owned();
                    state.buffer_data_size = part.data_size;
                }
                Ok(())
            }
            None => {
                // case 1
                state.buffer_data_size = 0;
                Ok(())
            }
        }
    }

    /**
     * Creates the part upload request, moving the active buffer's contents
     * into the request and then resetting the buffer.
     */
    fn create_upload_part_request(
        &self,
    ) -> Result<Option<UploadPartFluentBuilder>, gst::ErrorMessage> {
        let url = self.url.lock().unwrap();
        let mut state = self.state.lock().unwrap();
        let state = match *state {
            State::Started(ref mut started_state) => started_state,
            State::Completed => {
                unreachable!("Upload should not be completed yet");
            }
            State::Stopped => {
                unreachable!("Element should be started");
            }
        };

        if state.buffer_data_size == 0 {
            // Nothing to upload.
            return Ok(None);
        }

        // Update buffer len() to buffer_data_size, in the event the two
        // became out of sync because of seeking.  This ensures that the
        // data size is stored correctly in the cache and copied fully
        // into the request body.
        unsafe {
            state.buffer.set_len(state.buffer_data_size);
        }

        // Update/append the part cache
        state
            .cache
            .update_or_append(state.part_number as usize, &state.buffer);

        let capacity = state.buffer.capacity();
        let body = Some(ByteStream::from(std::mem::replace(
            &mut state.buffer,
            Vec::with_capacity(capacity),
        )));

        let bucket = Some(url.as_ref().unwrap().bucket.to_owned());
        let key = Some(url.as_ref().unwrap().object.to_owned());
        let upload_id = Some(state.upload_id.to_owned());

        let client = &state.client;
        let upload_part = client
            .upload_part()
            .set_body(body)
            .set_bucket(bucket)
            .set_key(key)
            .set_upload_id(upload_id)
            .set_part_number(Some(state.part_number as i32));

        Ok(Some(upload_part))
    }

    fn create_complete_multipart_upload_request(
        &self,
        started_state: &mut Started,
    ) -> CompleteMultipartUploadFluentBuilder {
        started_state
            .completed_parts
            .sort_by(|a, b| a.part_number.cmp(&b.part_number));

        let parts = Some(std::mem::take(&mut started_state.completed_parts));

        let completed_upload = CompletedMultipartUpload::builder().set_parts(parts).build();

        let url = self.url.lock().unwrap();
        let client = &started_state.client;

        let bucket = Some(url.as_ref().unwrap().bucket.to_owned());
        let key = Some(url.as_ref().unwrap().object.to_owned());
        let upload_id = Some(started_state.upload_id.to_owned());
        let multipart_upload = Some(completed_upload);

        client
            .complete_multipart_upload()
            .set_bucket(bucket)
            .set_key(key)
            .set_upload_id(upload_id)
            .set_multipart_upload(multipart_upload)
    }

    fn create_create_multipart_upload_request(
        &self,
        client: &Client,
        url: &GstS3Url,
        settings: &Settings,
    ) -> CreateMultipartUploadFluentBuilder {
        let bucket = Some(url.bucket.clone());
        let key = Some(url.object.clone());
        let content_type = settings.content_type.clone();
        let content_disposition = settings.content_disposition.clone();
        let metadata = settings.to_metadata(self);

        client
            .create_multipart_upload()
            .set_bucket(bucket)
            .set_key(key)
            .set_content_type(content_type)
            .set_content_disposition(content_disposition)
            .set_metadata(metadata)
    }

    fn create_abort_multipart_upload_request(
        &self,
        client: &Client,
        url: &GstS3Url,
        started_state: &Started,
    ) -> AbortMultipartUploadFluentBuilder {
        let bucket = Some(url.bucket.clone());
        let key = Some(url.object.clone());

        client
            .abort_multipart_upload()
            .set_bucket(bucket)
            .set_expected_bucket_owner(None)
            .set_key(key)
            .set_request_payer(None)
            .set_upload_id(Some(started_state.upload_id.to_owned()))
    }

    fn abort_multipart_upload_request(
        &self,
        started_state: &Started,
    ) -> Result<(), gst::ErrorMessage> {
        let s3url = {
            let url = self.url.lock().unwrap();
            match *url {
                Some(ref url) => url.clone(),
                None => unreachable!("Element should be started"),
            }
        };

        let client = &started_state.client;
        let abort_req = self.create_abort_multipart_upload_request(client, &s3url, started_state);
        let abort_req_future = abort_req.send();

        s3utils::wait(&self.abort_multipart_canceller, abort_req_future)
            .map(|_| ())
            .map_err(|err| match err {
                WaitError::FutureError(err) => {
                    gst::error_msg!(
                        gst::ResourceError::Write,
                        ["Failed to abort multipart upload: {err}: {}", err.meta()]
                    )
                }
                WaitError::Cancelled => {
                    gst::error_msg!(
                        gst::ResourceError::Write,
                        ["Abort multipart upload request interrupted."]
                    )
                }
            })
    }

    fn complete_multipart_upload_request(
        &self,
        started_state: &mut Started,
    ) -> Result<(), gst::ErrorMessage> {
        let complete_req = self.create_complete_multipart_upload_request(started_state);
        let complete_req_future = complete_req.send();

        s3utils::wait(&self.canceller, complete_req_future)
            .map(|_| ())
            .map_err(|err| match err {
                WaitError::FutureError(err) => gst::error_msg!(
                    gst::ResourceError::Write,
                    ["Failed to complete multipart upload: {err}: {}", err.meta()]
                ),
                WaitError::Cancelled => {
                    gst::error_msg!(
                        gst::LibraryError::Failed,
                        ["Complete multipart upload request interrupted"]
                    )
                }
            })
    }

    fn finalize_upload(&self) -> Result<(), gst::ErrorMessage> {
        if self.flush_current_buffer().is_err() {
            return Err(gst::error_msg!(
                gst::ResourceError::Settings,
                ["Failed to flush internal buffer."]
            ));
        }

        let mut state = self.state.lock().unwrap();
        let started_state = match *state {
            State::Started(ref mut started_state) => started_state,
            State::Completed => {
                unreachable!("Upload should not be completed yet");
            }
            State::Stopped => {
                unreachable!("Element should be started");
            }
        };

        let res = self.complete_multipart_upload_request(started_state);

        if res.is_ok() {
            *state = State::Completed;
        }

        res
    }

    fn start(&self) -> Result<(), gst::ErrorMessage> {
        let mut state = self.state.lock().unwrap();
        let settings = self.settings.lock().unwrap();

        if let State::Started { .. } = *state {
            unreachable!("Element should be started");
        }

        let s3url = {
            let url = self.url.lock().unwrap();
            match *url {
                Some(ref url) => url.clone(),
                None => {
                    return Err(gst::error_msg!(
                        gst::ResourceError::Settings,
                        ["Cannot start without a URL being set"]
                    ));
                }
            }
        };

        let timeout_config = s3utils::timeout_config(settings.request_timeout);

        let cred = match (
            settings.access_key.as_ref(),
            settings.secret_access_key.as_ref(),
        ) {
            (Some(access_key), Some(secret_access_key)) => Some(Credentials::new(
                access_key.clone(),
                secret_access_key.clone(),
                settings.session_token.clone(),
                None,
                "aws-s3-sink",
            )),
            _ => None,
        };

        let sdk_config =
            s3utils::wait_config(&self.canceller, s3url.region.clone(), timeout_config, cred)
                .map_err(|err| match err {
                    WaitError::FutureError(err) => gst::error_msg!(
                        gst::ResourceError::OpenWrite,
                        ["Failed to create SDK config: {err}"]
                    ),
                    WaitError::Cancelled => {
                        gst::error_msg!(
                            gst::LibraryError::Failed,
                            ["SDK config request interrupted during start"]
                        )
                    }
                })?;

        let config_builder = config::Builder::from(&sdk_config)
            .force_path_style(settings.force_path_style)
            .retry_config(RetryConfig::standard().with_max_attempts(settings.retry_attempts));

        let config = if let Some(ref uri) = settings.endpoint_uri {
            config_builder.endpoint_url(uri).build()
        } else {
            config_builder.build()
        };

        let client = Client::from_conf(config);

        let create_multipart_req =
            self.create_create_multipart_upload_request(&client, &s3url, &settings);
        let create_multipart_req_future = create_multipart_req.send();

        let response = s3utils::wait(&self.canceller, create_multipart_req_future).map_err(
            |err| match err {
                WaitError::FutureError(err) => gst::error_msg!(
                    gst::ResourceError::OpenWrite,
                    ["Failed to create multipart upload: {err}: {}", err.meta()]
                ),
                WaitError::Cancelled => {
                    gst::error_msg!(
                        gst::LibraryError::Failed,
                        ["Create multipart request interrupted during start"]
                    )
                }
            },
        )?;

        let upload_id = response.upload_id.ok_or_else(|| {
            gst::error_msg!(
                gst::ResourceError::Failed,
                ["Failed to get multipart upload ID"]
            )
        })?;

        *state = State::Started(Started::new(
            client,
            Vec::with_capacity(settings.buffer_size as usize),
            upload_id,
            settings.num_cached_parts,
        ));

        Ok(())
    }

    /**
     * Add 'src' to the buffer.  If this exceeds the capacity of the buffer
     * (i.e., the configured AWS max part size), the buffer is flushed (uploaded)
     * and the remaining portion of src starts the next buffer (part).
     */
    fn update_buffer(&self, src: &[u8]) -> Result<(), Option<gst::ErrorMessage>> {
        let mut state = self.state.lock().unwrap();
        let started_state = match *state {
            State::Started(ref mut started_state) => started_state,
            State::Completed => {
                unreachable!("Upload should not be completed yet");
            }
            State::Stopped => {
                unreachable!("Element should be started already");
            }
        };

        let to_copy = std::cmp::min(
            started_state.buffer.capacity() - started_state.buffer.len(),
            src.len(),
        );

        if *self.write_will_cache_miss.lock().unwrap() && to_copy > 0 {
            return Err(Some(gst::error_msg!(
                gst::ResourceError::NotFound,
                ["Cache miss on write has occurred"]
            )));
        }

        let (head, tail) = src.split_at(to_copy);
        started_state.buffer.extend_from_slice(head);
        started_state.buffer_data_size = started_state
            .buffer_data_size
            .max(started_state.buffer.len());
        let do_flush = started_state.buffer.capacity() == started_state.buffer.len();
        drop(state);

        if do_flush {
            self.flush_current_buffer()?;
        }

        if tail.len() > 0 {
            self.update_buffer(tail)?;
        }

        Ok(())
    }

    fn set_uri(self: &S3Sink, url_str: Option<&str>) -> Result<(), glib::Error> {
        let state = self.state.lock().unwrap();

        if let State::Started { .. } = *state {
            return Err(glib::Error::new(
                gst::URIError::BadState,
                "Cannot set URI on a started s3sink",
            ));
        }

        let mut url = self.url.lock().unwrap();

        if url_str.is_none() {
            *url = None;
            return Ok(());
        }

        gst::debug!(CAT, imp: self, "Setting uri to {:?}", url_str);

        let url_str = url_str.unwrap();
        match parse_s3_url(url_str) {
            Ok(s3url) => {
                *url = Some(s3url);
                Ok(())
            }
            Err(_) => Err(glib::Error::new(
                gst::URIError::BadUri,
                "Could not parse URI",
            )),
        }
    }

    /**
     * Attempt to seek to some location within the overall upload.
     * At this time, the only seeking that can happen is to places
     * within the cached parts.  If the seek is outside of the cached
     * parts, the seek will fail.  The cases to evaluate are:
     *   1. new_offset == current position:
     *      return true (do nothing)
     *   2. new_offset is within the current buffer:
     *      change the write head position on the buffer
     *      return true
     *   3. new_offset is within the cache:
     *      flush
     *      switch to cached buffer, part number
     *      change the write head position on the buffer
     */
    fn seek(self: &S3Sink, new_offset: u64) -> Result<(), Option<gst::ErrorMessage>> {
        let mut state = self.state.lock().unwrap();
        let started_state = match *state {
            State::Started(ref mut started_state) => started_state,
            State::Completed => {
                unreachable!("Upload should not be completed yet");
            }
            State::Stopped => {
                unreachable!("Element should be started");
            }
        };

        // Case 1: no-op.
        if started_state.upload_pos == new_offset {
            return Ok(());
        }

        // Determine if new_offset is within the current part or one in the cache.
        let part_start =
            (started_state.part_number as u64 - 1) * started_state.buffer.capacity() as u64;
        let part_end = part_start + started_state.buffer_data_size as u64;
        let part_limits = part_start..part_end;

        gst::trace!(CAT, imp: self, "Current part {} {part_limits:?} - seeking to {new_offset}", started_state.part_number);

        let mut next_part = 0;
        let cache_result = started_state.cache.find(new_offset, &mut next_part);

        let offset_in_buffer = new_offset as usize % started_state.buffer.capacity();

        if part_limits.contains(&new_offset) {
            gst::trace!(CAT, imp: self, "Seeking to offset {} within current buffer", new_offset);
            started_state.buffer_data_size = started_state
                .buffer_data_size
                .max(started_state.buffer.len());
            started_state.upload_pos = new_offset;
            unsafe {
                started_state.buffer.set_len(offset_in_buffer);
            }

            return Ok(());
        } else if cache_result.is_ok() {
            let result = cache_result.unwrap();
            let next_buffer = result.buffer.to_vec();
            let next_size = result.data_size.to_owned();

            if 0 < next_buffer.len() {
                // cache hit
                drop(state);
                self.flush_current_buffer()?;

                // Flushed okay, lock state again and update the buffer
                let mut state = self.state.lock().unwrap();
                let started_state = match *state {
                    State::Started(ref mut started_state) => started_state,
                    _ => unreachable!("Element should still be started"),
                };

                // Clear the cache miss flag
                *self.write_will_cache_miss.lock().unwrap() = false;

                gst::trace!(CAT, imp: self, "Seeking to offset {} within the cache (part {})", new_offset, next_part);

                started_state.part_number = next_part.try_into().unwrap();
                started_state.buffer = next_buffer;
                started_state.buffer_data_size = next_size;
                started_state.upload_pos = new_offset;
                unsafe {
                    started_state.buffer.set_len(offset_in_buffer);
                }

                return Ok(());
            } else {
                // cache miss -- the part information is known but no data was stored.
                return Err(Some(gst::error_msg!(
                    gst::StreamError::Failed,
                    ["Buffer not cached for part {next_part}"]
                )));
            }
        } else {
            // Seek requested to an unsupported location.
            return Err(Some(gst::error_msg!(
                gst::StreamError::Failed,
                ["Attempted to seek into unreachable region"]
            )));
        }
    }
}

#[glib::object_subclass]
impl ObjectSubclass for S3Sink {
    const NAME: &'static str = "GstAwsS3Sink";
    type Type = super::S3Sink;
    type ParentType = gst_base::BaseSink;
    type Interfaces = (gst::URIHandler,);
}

impl ObjectImpl for S3Sink {
    fn constructed(&self) {
        self.parent_constructed();

        *self.eos_pending.lock().unwrap() = false;
        *self.write_will_cache_miss.lock().unwrap() = false;
        self.obj().set_sync(false);
    }

    fn properties() -> &'static [glib::ParamSpec] {
        static PROPERTIES: Lazy<Vec<glib::ParamSpec>> = Lazy::new(|| {
            // AWS min/max is 5 MB -> 5GB
            // Rust Vec, used in this module, has a maximum size of usize, which is
            // tied to the system architecture.  Per this, all tier 1 architectures
            // are 32 or 64-bit, so the 5 MB minimum is no big deal:
            //     https://doc.rust-lang.org/nightly/rustc/platform-support.html
            //
            // However the 5 GB max would exceed 32-bit architectures.
            let min_buffer_size: u64 = 5 * 1024_u64.pow(2);
            let max_buffer_size: u64 = std::cmp::min(std::usize::MAX as u64, 5 * 1024_u64.pow(3));
            let default_buffer_size: u64 = min_buffer_size;
            vec![
                glib::ParamSpecString::builder("bucket")
                    .nick("S3 Bucket")
                    .blurb("The bucket of the file to write")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("key")
                    .nick("S3 Key")
                    .blurb("The key of the file to write")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("region")
                    .nick("AWS Region")
                    .blurb("An AWS region (e.g. eu-west-2).")
                    .default_value(Some("us-west-2"))
                    .mutable_ready()
                    .build(),
                glib::ParamSpecUInt64::builder("part-size")
                    .nick("Part size")
                    .blurb("A size (in bytes) of an individual part used for multipart upload.")
                    .minimum(min_buffer_size)
                    .maximum(max_buffer_size)
                    .default_value(default_buffer_size)
                    .construct()
                    .mutable_ready()
                    .build(),
                glib::ParamSpecInt64::builder("num-cached-parts")
                    .nick("Number of parts to cache (seeking)")
                    .blurb("Number of parts to cache to enable seeking before the multipart upload completes")
                    .minimum(-1 * MAX_MULTIPART_NUMBER)
                    .maximum(MAX_MULTIPART_NUMBER)
                    .default_value(DEFAULT_NUM_CACHED_PARTS)
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("uri")
                    .nick("URI")
                    .blurb("The S3 object URI")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("access-key")
                    .nick("Access Key")
                    .blurb("AWS Access Key")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("secret-access-key")
                    .nick("Secret Access Key")
                    .blurb("AWS Secret Access Key")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("session-token")
                    .nick("Session Token")
                    .blurb("AWS temporary Session Token from STS")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecBoxed::builder::<gst::Structure>("metadata")
                    .nick("Metadata")
                    .blurb("A map of metadata to store with the object in S3; field values need to be convertible to strings.")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecEnum::builder_with_default("on-error", DEFAULT_MULTIPART_UPLOAD_ON_ERROR)
                    .nick("Whether to upload or complete the multipart upload on error")
                    .blurb("Do nothing, abort or complete a multipart upload request on error")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecUInt::builder("retry-attempts")
                    .nick("Retry attempts")
                    .blurb("Number of times AWS SDK attempts a request before abandoning the request")
                    .minimum(1)
                    .maximum(10)
                    .default_value(DEFAULT_RETRY_ATTEMPTS)
                    .build(),
                glib::ParamSpecInt64::builder("request-timeout")
                    .nick("Request timeout")
                    .blurb("Timeout for general S3 requests (in ms, set to -1 for infinity)")
                    .minimum(-1)
                    .default_value(DEFAULT_REQUEST_TIMEOUT_MSEC as i64)
                    .build(),
                glib::ParamSpecInt64::builder("upload-part-request-timeout")
                    .nick("Upload part request timeout")
                    .blurb("Timeout for a single upload part request (in ms, set to -1 for infinity) (Deprecated. Use request-timeout.)")
                    .minimum(-1)
                    .default_value(DEFAULT_UPLOAD_PART_REQUEST_TIMEOUT_MSEC as i64)
                    .build(),
                glib::ParamSpecInt64::builder("complete-upload-request-timeout")
                    .nick("Complete upload request timeout")
                    .blurb("Timeout for the complete multipart upload request (in ms, set to -1 for infinity) (Deprecated. Use request-timeout.)")
                    .minimum(-1)
                    .default_value(DEFAULT_COMPLETE_REQUEST_TIMEOUT_MSEC as i64)
                    .build(),
                glib::ParamSpecInt64::builder("retry-duration")
                    .nick("Retry duration")
                    .blurb("How long we should retry general S3 requests before giving up (in ms, set to -1 for infinity) (Deprecated. Use retry-attempts.)")
                    .minimum(-1)
                    .default_value(DEFAULT_RETRY_DURATION_MSEC as i64)
                    .build(),
                glib::ParamSpecInt64::builder("upload-part-retry-duration")
                    .nick("Upload part retry duration")
                    .blurb("How long we should retry upload part requests before giving up (in ms, set to -1 for infinity) (Deprecated. Use retry-attempts.)")
                    .minimum(-1)
                    .default_value(DEFAULT_UPLOAD_PART_RETRY_DURATION_MSEC as i64)
                    .build(),
                glib::ParamSpecInt64::builder("complete-upload-retry-duration")
                    .nick("Complete upload retry duration")
                    .blurb("How long we should retry complete multipart upload requests before giving up (in ms, set to -1 for infinity) (Deprecated. Use retry-attempts.)")
                    .minimum(-1)
                    .default_value(DEFAULT_COMPLETE_RETRY_DURATION_MSEC as i64)
                    .build(),
                glib::ParamSpecString::builder("endpoint-uri")
                    .nick("S3 endpoint URI")
                    .blurb("The S3 endpoint URI to use")
                    .build(),
                glib::ParamSpecString::builder("content-type")
                    .nick("content-type")
                    .blurb("Content-Type header to set for uploaded object")
                    .build(),
                glib::ParamSpecString::builder("content-disposition")
                    .nick("content-disposition")
                    .blurb("Content-Disposition header to set for uploaded object")
                    .build(),
                glib::ParamSpecBoolean::builder("force-path-style")
                    .nick("Force path style")
                    .blurb("Force client to use path-style addressing for buckets")
                    .default_value(DEFAULT_FORCE_PATH_STYLE)
                    .build(),
            ]
        });

        PROPERTIES.as_ref()
    }

    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        let mut settings = self.settings.lock().unwrap();

        gst::debug!(
            CAT,
            imp: self,
            "Setting property '{}' to '{:?}'",
            pspec.name(),
            value
        );

        match pspec.name() {
            "bucket" => {
                settings.bucket = value
                    .get::<Option<String>>()
                    .expect("type checked upstream");
                if settings.key.is_some() {
                    let _ = self.set_uri(Some(&settings.to_uri()));
                }
            }
            "key" => {
                settings.key = value
                    .get::<Option<String>>()
                    .expect("type checked upstream");
                if settings.bucket.is_some() {
                    let _ = self.set_uri(Some(&settings.to_uri()));
                }
            }
            "region" => {
                let region = value.get::<String>().expect("type checked upstream");
                settings.region = Region::new(region);
                if settings.key.is_some() && settings.bucket.is_some() {
                    let _ = self.set_uri(Some(&settings.to_uri()));
                }
            }
            "part-size" => {
                settings.buffer_size = value
                    .get::<u64>()
                    .expect("type checked upstream")
                    .try_into()
                    .unwrap();
            }
            "num-cached-parts" => {
                settings.num_cached_parts = value.get::<i64>().expect("type checked upstream");
            }
            "uri" => {
                let _ = self.set_uri(value.get().expect("type checked upstream"));
            }
            "access-key" => {
                settings.access_key = value.get().expect("type checked upstream");
            }
            "secret-access-key" => {
                settings.secret_access_key = value.get().expect("type checked upstream");
            }
            "session-token" => {
                settings.session_token = value.get().expect("type checked upstream");
            }
            "metadata" => {
                settings.metadata = value.get().expect("type checked upstream");
            }
            "on-error" => {
                settings.multipart_upload_on_error =
                    value.get::<OnError>().expect("type checked upstream");
            }
            "retry-attempts" => {
                settings.retry_attempts = value.get::<u32>().expect("type checked upstream");
            }
            "request-timeout" => {
                settings.request_timeout =
                    duration_from_millis(value.get::<i64>().expect("type checked upstream"));
            }
            "upload-part-request-timeout" => {
                settings.request_timeout =
                    duration_from_millis(value.get::<i64>().expect("type checked upstream"));
            }
            "complete-upload-request-timeout" => {
                settings.request_timeout =
                    duration_from_millis(value.get::<i64>().expect("type checked upstream"));
            }
            "retry-duration" => {
                /*
                 * To maintain backwards compatibility calculate retry attempts
                 * by dividing the provided duration from request timeout.
                 */
                let value = value.get::<i64>().expect("type checked upstream");
                let request_timeout = duration_to_millis(Some(settings.request_timeout));
                let retry_attempts = if value > request_timeout {
                    value / request_timeout
                } else {
                    1
                };
                settings.retry_attempts = retry_attempts as u32;
            }
            "upload-part-retry-duration" | "complete-upload-retry-duration" => {
                gst::warning!(CAT, "Use retry-attempts. retry/upload-part/complete-upload-retry duration are deprecated.");
            }
            "endpoint-uri" => {
                settings.endpoint_uri = value
                    .get::<Option<String>>()
                    .expect("type checked upstream");
                if settings.key.is_some() && settings.bucket.is_some() {
                    let _ = self.set_uri(Some(&settings.to_uri()));
                }
            }
            "content-type" => {
                settings.content_type = value
                    .get::<Option<String>>()
                    .expect("type checked upstream");
            }
            "content-disposition" => {
                settings.content_disposition = value
                    .get::<Option<String>>()
                    .expect("type checked upstream");
            }
            "force-path-style" => {
                settings.force_path_style = value.get::<bool>().expect("type checked upstream");
            }
            _ => unimplemented!(),
        }
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        let settings = self.settings.lock().unwrap();

        match pspec.name() {
            "key" => settings.key.to_value(),
            "bucket" => settings.bucket.to_value(),
            "region" => settings.region.to_string().to_value(),
            "part-size" => (settings.buffer_size as u64).to_value(),
            "num-cached-parts" => settings.num_cached_parts.to_value(),
            "uri" => {
                let url = self.url.lock().unwrap();
                let url = match *url {
                    Some(ref url) => url.to_string(),
                    None => "".to_string(),
                };

                url.to_value()
            }
            "access-key" => settings.access_key.to_value(),
            "secret-access-key" => settings.secret_access_key.to_value(),
            "session-token" => settings.session_token.to_value(),
            "metadata" => settings.metadata.to_value(),
            "on-error" => settings.multipart_upload_on_error.to_value(),
            "retry-attempts" => settings.retry_attempts.to_value(),
            "request-timeout" => duration_to_millis(Some(settings.request_timeout)).to_value(),
            "upload-part-request-timeout" => {
                duration_to_millis(Some(settings.request_timeout)).to_value()
            }
            "complete-upload-request-timeout" => {
                duration_to_millis(Some(settings.request_timeout)).to_value()
            }
            "retry-duration" | "upload-part-retry-duration" | "complete-upload-retry-duration" => {
                let request_timeout = duration_to_millis(Some(settings.request_timeout));
                (settings.retry_attempts as i64 * request_timeout).to_value()
            }
            "endpoint-uri" => settings.endpoint_uri.to_value(),
            "content-type" => settings.content_type.to_value(),
            "content-disposition" => settings.content_disposition.to_value(),
            "force-path-style" => settings.force_path_style.to_value(),
            _ => unimplemented!(),
        }
    }
}

impl GstObjectImpl for S3Sink {}

impl ElementImpl for S3Sink {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static ELEMENT_METADATA: Lazy<gst::subclass::ElementMetadata> = Lazy::new(|| {
            #[cfg(feature = "doc")]
            OnError::static_type().mark_as_plugin_api(gst::PluginAPIFlags::empty());
            gst::subclass::ElementMetadata::new(
                "Amazon S3 sink",
                "Source/Network",
                "Writes an object to Amazon S3",
                "Marcin Kolny <mkolny@amazon.com>",
            )
        });

        Some(&*ELEMENT_METADATA)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
        static PAD_TEMPLATES: Lazy<Vec<gst::PadTemplate>> = Lazy::new(|| {
            let caps = gst::Caps::new_any();
            let sink_pad_template = gst::PadTemplate::new(
                "sink",
                gst::PadDirection::Sink,
                gst::PadPresence::Always,
                &caps,
            )
            .unwrap();

            vec![sink_pad_template]
        });

        PAD_TEMPLATES.as_ref()
    }
}

impl URIHandlerImpl for S3Sink {
    const URI_TYPE: gst::URIType = gst::URIType::Sink;

    fn protocols() -> &'static [&'static str] {
        &["s3"]
    }

    fn uri(&self) -> Option<String> {
        self.url.lock().unwrap().as_ref().map(|s| s.to_string())
    }

    fn set_uri(&self, uri: &str) -> Result<(), glib::Error> {
        self.set_uri(Some(uri))
    }
}

impl BaseSinkImpl for S3Sink {
    fn start(&self) -> Result<(), gst::ErrorMessage> {
        self.start()
    }

    fn stop(&self) -> Result<(), gst::ErrorMessage> {
        let mut state = self.state.lock().unwrap();

        if let State::Started(ref mut state) = *state {
            gst::warning!(CAT, imp: self, "Stopped without EOS");

            // We're stopping without an EOS -- treat this as an error and deal with the open
            // multipart upload accordingly _if_ we managed to upload any parts
            if !state.completed_parts.is_empty() {
                self.flush_multipart_upload(state);
            }
        }

        *state = State::Stopped;
        gst::info!(CAT, imp: self, "Stopped");

        Ok(())
    }

    fn render(&self, buffer: &gst::Buffer) -> Result<gst::FlowSuccess, gst::FlowError> {
        if let State::Stopped = *self.state.lock().unwrap() {
            gst::element_imp_error!(self, gst::CoreError::Failed, ["Not started yet"]);
            return Err(gst::FlowError::Error);
        }

        if let State::Completed = *self.state.lock().unwrap() {
            gst::element_imp_error!(
                self,
                gst::CoreError::Failed,
                ["Trying to render after upload complete"]
            );
            return Err(gst::FlowError::Error);
        }

        gst::trace!(CAT, imp: self, "Rendering {:?}", buffer);
        let map = buffer.map_readable().map_err(|_| {
            gst::element_imp_error!(self, gst::CoreError::Failed, ["Failed to map buffer"]);
            gst::FlowError::Error
        })?;

        match self.update_buffer(&map) {
            Ok(_) => Ok(gst::FlowSuccess::Ok),
            Err(err) => match err {
                Some(error_message) => {
                    gst::error!(CAT, imp: self, "Multipart upload failed: {}", error_message);
                    self.post_error_message(error_message);
                    Err(gst::FlowError::Error)
                }
                _ => {
                    gst::info!(CAT, imp: self, "Upload interrupted. Flushing...");
                    Err(gst::FlowError::Flushing)
                }
            },
        }
    }

    fn unlock(&self) -> Result<(), gst::ErrorMessage> {
        let mut canceller = self.canceller.lock().unwrap();
        let mut abort_canceller = self.abort_multipart_canceller.lock().unwrap();
        canceller.abort();
        abort_canceller.abort();
        Ok(())
    }

    fn unlock_stop(&self) -> Result<(), gst::ErrorMessage> {
        let mut canceller = self.canceller.lock().unwrap();
        let mut abort_canceller = self.abort_multipart_canceller.lock().unwrap();
        *canceller = s3utils::Canceller::None;
        *abort_canceller = s3utils::Canceller::None;
        Ok(())
    }

    fn query(&self, query: &mut gst::QueryRef) -> bool {
        match query.view_mut() {
            gst::QueryViewMut::Formats(fmt) => {
                fmt.set(&vec![gst::Format::Bytes]);
                true
            }
            gst::QueryViewMut::Position(pos) => {
                if pos.format() == gst::Format::Bytes {
                    let mut state = self.state.lock().unwrap();
                    match *state {
                        State::Started(ref mut started_state) => {
                            pos.set(gst::format::Bytes::from_u64(started_state.upload_pos));
                            true
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            }
            gst::QueryViewMut::Seeking(seek) => {
                let (seekable, start, stop) = match seek.format() {
                    gst::Format::Bytes => {
                        let mut state = self.state.lock().unwrap();
                        match *state {
                            State::Started(ref mut started_state) => {
                                // TODO: unclear how to really respond here since  it is
                                // possible to seek to within the range of the cache OR to
                                // within the range of the currently active buffer, and when
                                // the currently active buffer is not in the bounds of what
                                // is in the cache, there's no way to provide both sets of
                                // limits to the reply.  So if cache is enabled, give an
                                // affirmative "anywhere" response and we'll argue this point
                                // when dealing with segment events.
                                let mut max = 0_u64;
                                if started_state.cache.max_depth > 0 {
                                    max = u64::MAX - 1;
                                }
                                (true, 0, max)
                            }
                            _ => (false, 0, 0),
                        }
                    }
                    _ => (false, 0, 0),
                };

                seek.set(
                    seekable,
                    gst::format::Bytes::from_u64(start),
                    gst::format::Bytes::from_u64(stop),
                );
                // "no description available" in docs for return from query
                // I'm going to assume 'true' here since the call did successfully
                // determine if 'seekable' needs to be true or false and is set on
                // the query.
                true
            }
            _ => BaseSinkImplExt::parent_query(self, query),
        }
    }

    fn event(&self, event: gst::Event) -> bool {
        if let gst::EventView::Eos(_) = event.view() {
            *self.eos_pending.lock().unwrap() = true;
            *self.write_will_cache_miss.lock().unwrap() = false;
            if let Err(error_message) = self.finalize_upload() {
                gst::error!(
                    CAT,
                    imp: self,
                    "Failed to finalize the upload: {}",
                    error_message
                );
                return false;
            }
        } else if let gst::EventView::Segment(event) = event.view() {
            let mut state = self.state.lock().unwrap();
            match *state {
                State::Started(ref mut started_state) => started_state,
                State::Completed => {
                    unreachable!("Upload should not be completed yet");
                }
                State::Stopped => {
                    unreachable!("Element should be started already");
                }
            };
            if event.segment().format() == gst::Format::Bytes {
                let segment = event.segment();
                drop(state);
                // value() is an i64, however the docs do not state what a negative bytes offset
                // would imply since in practice it seems to always be an absolute offset from 0.
                // If we get a negative number here, let it barf here.
                if let Err(error_message) = self.seek(segment.start().value().try_into().unwrap()) {
                    gst::error!(CAT, imp: self, "Failed to seek: {:?}", error_message);
                    return false;
                }
            }
        }

        BaseSinkImplExt::parent_event(self, event)
    }
}
