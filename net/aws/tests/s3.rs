// Copyright (C) 2022, Asymptotic Inc.
//      Author: Arun Raghavan <arun@asymptotic.io>
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// <https://mozilla.org/MPL/2.0/>.
//
// SPDX-License-Identifier: MPL-2.0
//

// The test times out on Windows for some reason, skip until we figure out why
#[cfg(not(target_os = "windows"))]
#[cfg(test)]
mod tests {
    use gst::prelude::*;

    use gstaws::s3utils::{AWS_BEHAVIOR_VERSION, DEFAULT_S3_REGION};

    fn init() {
        use std::sync::Once;
        static INIT: Once = Once::new();

        INIT.call_once(|| {
            gst::init().unwrap();
            gstaws::plugin_register_static().unwrap();
            // Makes it easier to get AWS SDK logs if needed
            env_logger::init();
        });
    }

    fn make_buffer(content: &[u8]) -> gst::Buffer {
        let mut buf = gst::Buffer::from_slice(content.to_owned());
        buf.make_mut().set_pts(gst::ClockTime::from_mseconds(200));
        buf
    }

    async fn delete_object(region: String, bucket: &str, key: &str) {
        let region_provider = aws_config::meta::region::RegionProviderChain::first_try(
            aws_sdk_s3::config::Region::new(region),
        )
        .or_default_provider();

        let config = aws_config::defaults(AWS_BEHAVIOR_VERSION.clone())
            .region(region_provider)
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&config);

        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
    }

    fn get_env_args(key_prefix: &str) -> (String, String, String) {
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| DEFAULT_S3_REGION.to_string());
        let bucket =
            std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "gst-plugins-rs-tests".to_string());
        let key = format!("{key_prefix}-{:?}.txt", chrono::Utc::now());
        (region, bucket, key)
    }

    fn get_uri(region: &str, bucket: &str, key: &str) -> String {
        format!("s3://{region}/{bucket}/{key}")
    }

    // Common helper
    async fn do_s3_multipart_test(key_prefix: &str) {
        init();

        let (region, bucket, key) = get_env_args(key_prefix);
        let uri = get_uri(&region, &bucket, &key);
        let content = "Hello, world!\n".as_bytes();

        // Manually add the element so we can configure it before it goes to PLAYING
        let mut h1 = gst_check::Harness::new_empty();
        // Need to add_parse() because the Harness API / Rust bindings aren't conducive to creating and
        // adding an element manually

        h1.add_parse(format!("awss3sink uri=\"{uri}\"").as_str());

        h1.set_src_caps(gst::Caps::builder("text/plain").build());
        h1.play();

        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();
        h1.push_event(gst::event::Eos::new());

        let mut h2 = gst_check::Harness::new("awss3src");
        h2.element().unwrap().set_property("uri", uri.clone());
        h2.play();

        let buf = h2.pull_until_eos().unwrap().unwrap();
        assert_eq!(
            content.repeat(5),
            buf.into_mapped_buffer_readable().unwrap().as_slice()
        );

        delete_object(region.clone(), &bucket, &key).await;
    }

    // Common helper
    async fn do_s3_putobject_test(
        key_prefix: &str,
        buffers: Option<u64>,
        bytes: Option<u64>,
        time: Option<gst::ClockTime>,
        do_eos: bool,
    ) {
        init();

        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| DEFAULT_S3_REGION.to_string());
        let bucket =
            std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "gst-plugins-rs-tests".to_string());
        let key = format!("{key_prefix}-{:?}.txt", chrono::Utc::now());
        let uri = format!("s3://{region}/{bucket}/{key}");
        let content = "Hello, world!\n".as_bytes();

        // Manually add the element so we can configure it before it goes to PLAYING
        let mut h1 = gst_check::Harness::new_empty();

        // Need to add_parse() because the Harness API / Rust bindings aren't conducive to creating and
        // adding an element manually

        h1.add_parse(
            format!("awss3putobjectsink key=\"{key}\" region=\"{region}\" bucket=\"{bucket}\" name=\"sink\"")
                .as_str(),
        );

        let h1el = h1
            .element()
            .unwrap()
            .dynamic_cast::<gst::Bin>()
            .unwrap()
            .by_name("sink")
            .unwrap();
        if let Some(b) = buffers {
            h1el.set_property("flush-interval-buffers", b)
        };
        if let Some(b) = bytes {
            h1el.set_property("flush-interval-bytes", b)
        };
        if time.is_some() {
            h1el.set_property("flush-interval-time", time)
        };
        if !do_eos {
            h1el.set_property("flush-on-error", true)
        }

        h1.set_src_caps(gst::Caps::builder("text/plain").build());
        h1.play();

        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();
        h1.push(make_buffer(content)).unwrap();

        if do_eos {
            h1.push_event(gst::event::Eos::new());
        } else {
            // teardown to trigger end
            drop(h1);
        }

        let mut h2 = gst_check::Harness::new("awss3src");
        h2.element().unwrap().set_property("uri", uri.clone());
        h2.play();

        let buf = h2.pull_until_eos().unwrap().unwrap();
        assert_eq!(
            content.repeat(5),
            buf.into_mapped_buffer_readable().unwrap().as_slice()
        );

        delete_object(region.clone(), &bucket, &key).await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_simple() {
        do_s3_multipart_test("s3-test").await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_whitespace() {
        do_s3_multipart_test("s3 test").await;
    }

    /**
     * This test will run the sink with 1 part cached at the head (start)
     * of the upload.  It will push 12 packets (just over 2 parts), seek to
     * a few bytes into the first part, write a change, then EOS.  The
     * expected result is the seek to use the cache, and the EOS to push the
     * cached buffer and complete the upload.
     */
    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_head_cached() {
        init();

        let (region, bucket, key) = get_env_args("head_cached");
        let uri = get_uri(&region, &bucket, &key);
        let buffer_size = 1024 * 1024;
        let buffers_per_part = 5;
        let part_size = buffer_size * buffers_per_part;
        let num_buffers = 12;

        let mut h1 = gst_check::Harness::new_parse(&format!(
            "awss3sink name=\"sink\" uri=\"{uri}\" num-cached-parts=1 part-size={part_size}"
        ));
        h1.set_src_caps(gst::Caps::builder("text/plain").build());
        h1.play();

        // Push stream start, segment, and buffers
        let mut segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        h1.push_event(gst::event::StreamStart::builder(&"test-stream").build());
        h1.push_event(gst::event::Segment::new(&segment));
        for i in 1..=num_buffers as u8 {
            let buffer = make_buffer(&vec![i; buffer_size]);
            h1.push(buffer).unwrap();
        }

        // Try to seek into part 1 (end of first packet).
        segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        segment.set_start(gst::format::Bytes::from_u64(
            buffer_size.try_into().unwrap(),
        ));
        assert!(h1.push_event(gst::event::Segment::new(&segment)));

        // Overwrite second packet of part 1: [01...][AA...][03...]...
        // This should succeed.
        h1.push(make_buffer(&vec![0xAA; buffer_size])).unwrap();

        // EOS to finish the upload
        h1.push_event(gst::event::Eos::new());

        // FIXME: This seems to return too early -- before the file is finalized
        // at S3 -- because the h2.play() occasionally fails on GstState change
        // to playing because the file is not found.

        //  Download and verify contents
        let mut h2 = gst_check::Harness::new("awss3src");
        h2.element().unwrap().set_property("uri", uri.clone());
        h2.play();

        let mut location: usize = 0;
        let mut expect = 0x00_u8;
        loop {
            match h2.pull() {
                Ok(temp) => {
                    let buffer = temp.into_mapped_buffer_readable().unwrap();

                    if 0 == location % buffer_size {
                        expect += 1;
                    }

                    for b in buffer.as_slice() {
                        if buffer_size <= location && location < 2 * buffer_size {
                            assert_eq!(b, &0xAA_u8);
                        } else {
                            assert_eq!(b, &expect);
                        }
                        location += 1;
                    }
                }
                Err(_) => break,
            }
        }
        assert_eq!(location, num_buffers * buffer_size);

        delete_object(region.clone(), &bucket, &key).await;
    }

    /**
     * Pulls all buffers via harness.pull() until the 60-second timeout occurs,
     * causing this method to return a buffer containing all of the others.
     */
    fn pull_all_buffers_as_slice(harness: &mut gst_check::Harness) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();

        loop {
            match harness.pull() {
                Ok(temp) => {
                    let buffer = temp.into_mapped_buffer_readable().unwrap();
                    out.extend(buffer.as_slice());
                }
                Err(_) => break,
            }
        }

        out
    }

    /**
     * This test verifies that a seek within the current part is permitted.
     * Since the cache is not involved, subsequent writes beyond the previous "end" of
     * of the buffer will simply continue as normal.
     */
    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_permit_seek_in_active_part() {
        init();

        let (region, bucket, key) = get_env_args("seek_active_part");
        let uri = get_uri(&region, &bucket, &key);
        let buffer_size = 1024 * 1024;
        let buffers_per_part = 5;
        let part_size = buffer_size * buffers_per_part;
        let num_buffers = 2;

        let mut h1 = gst_check::Harness::new_parse(&format!(
            "awss3sink name=\"sink\" uri=\"{uri}\" num-cached-parts=1 part-size={part_size}"
        ));
        h1.set_src_caps(gst::Caps::builder("text/plain").build());
        h1.play();

        // Push stream start, segment, and buffers
        let mut segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        h1.push_event(gst::event::StreamStart::builder(&"test-stream").build());
        h1.push_event(gst::event::Segment::new(&segment));
        for i in 1..=num_buffers as u8 {
            let buffer = make_buffer(&vec![i; buffer_size]);
            h1.push(buffer).unwrap();
        }

        // Seek back to the start of the second buffer, which is still the active part.
        segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        segment.set_start(gst::format::Bytes::from_u64(
            buffer_size.try_into().unwrap(),
        ));
        assert!(h1.push_event(gst::event::Segment::new(&segment)));

        // Overwrite that buffer, which should succeed.
        // This should succeed.
        h1.push(make_buffer(&vec![0xAA; buffer_size])).unwrap();

        // EOS to finish the upload
        h1.push_event(gst::event::Eos::new());

        // Verify
        let mut h2 = gst_check::Harness::new("awss3src");
        h2.element().unwrap().set_property("uri", uri.clone());
        h2.play();

        let buffer = pull_all_buffers_as_slice(&mut h2);
        assert_eq!(buffer.len(), buffer_size * 2);
        assert!(buffer[0..(buffer_size - 1)].iter().all(|&b| b == 0x01_u8));
        assert!(buffer[buffer_size..].iter().all(|&b| b == 0xAA_u8));

        delete_object(region.clone(), &bucket, &key).await;
    }

    /**
     * This test verifies the behavior where a seek back into the cache is successful,
     * but the continued write goes off the edge of the cache.  Since this implementation
     * does not attempt to do any download/re-upload of previous parts, the expected result
     * is a flow error when the boundary is crossed.
     *
     * The test configures 1 part head cache, writes two parts, seeks into the tail of the
     * first part and writes a buffer that would cross the part boundary (cache miss).
     * The expected result is a flow error.
     */
    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_cache_miss_on_write() {
        init();

        let (region, bucket, key) = get_env_args("cache_miss_on_write");
        let uri = get_uri(&region, &bucket, &key);
        let buffer_size = 1024 * 1024;
        let buffers_per_part = 5;
        let part_size = buffer_size * buffers_per_part;
        let num_buffers = 6;

        let mut h1 = gst_check::Harness::new_parse(&format!(
            "awss3sink name=\"sink\" uri=\"{uri}\" num-cached-parts=1 part-size={part_size}"
        ));
        h1.set_src_caps(gst::Caps::builder("text/plain").build());
        h1.play();

        // Push stream start, segment, and buffers
        let mut segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        h1.push_event(gst::event::StreamStart::builder(&"test-stream").build());
        h1.push_event(gst::event::Segment::new(&segment));
        for i in 1..=num_buffers as u8 {
            let buffer = make_buffer(&vec![i; buffer_size]);
            h1.push(buffer).unwrap();
        }

        // Seek to near the end of part 1 (half a buffer from tail)
        segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        segment.set_start(gst::format::Bytes::from_u64(
            (part_size - (buffer_size / 2)).try_into().unwrap(),
        ));
        assert!(h1.push_event(gst::event::Segment::new(&segment)));

        // Write a full buffer.  The first half of the write will succeed internally but the continued
        // write will trigger a flow error response since the cache boundary will be crossed.
        assert!(h1.push(make_buffer(&vec![0xAA; buffer_size])).is_err());
    }

    /**
     * This test caches the first part.  Two parts are written, then a seek into the tail of
     * the first part.  The subsequent write reaches the end of part 1, flagging a potential
     * cache miss is imminent if more data is written.  However, an EOS event is received
     * which successfully finalizes the file and does not result in a flow error.
     */
    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_eos_on_cache_boundary() {
        init();

        let (region, bucket, key) = get_env_args("eos_on_cache_boundary");
        let uri = get_uri(&region, &bucket, &key);
        let buffer_size = 1024 * 1024;
        let buffers_per_part = 5;
        let part_size = buffer_size * buffers_per_part;
        let num_buffers = 6;

        let mut h1 = gst_check::Harness::new_parse(&format!(
            "awss3sink name=\"sink\" uri=\"{uri}\" num-cached-parts=1 part-size={part_size}"
        ));
        h1.set_src_caps(gst::Caps::builder("text/plain").build());
        h1.play();

        // Push stream start, segment, and buffers
        let mut segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        h1.push_event(gst::event::StreamStart::builder(&"test-stream").build());
        h1.push_event(gst::event::Segment::new(&segment));
        for i in 1..=num_buffers as u8 {
            let buffer = make_buffer(&vec![i; buffer_size]);
            h1.push(buffer).unwrap();
        }

        // Seek to the end of part 1 (a buffer from tail)
        segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        segment.set_start(gst::format::Bytes::from_u64(
            (part_size - buffer_size).try_into().unwrap(),
        ));
        assert!(h1.push_event(gst::event::Segment::new(&segment)));

        // Write a full buffer.  The write should succeed because it only reaches the cache boundary.
        assert!(h1.push(make_buffer(&vec![0xAA; buffer_size])).is_ok());

        // EOS should be allowed and result in a finalized file.
        h1.push_event(gst::event::Eos::new());

        // Verify
        let mut h2 = gst_check::Harness::new("awss3src");
        h2.element().unwrap().set_property("uri", uri.clone());
        h2.play();

        let buffer = pull_all_buffers_as_slice(&mut h2);
        assert_eq!(buffer.len(), buffer_size * num_buffers);
        assert!(buffer[buffer_size * 3..(buffer_size * 4 - 1)]
            .iter()
            .all(|&b| b == 0x04_u8));
        assert!(buffer[buffer_size * 4..(buffer_size * 5 - 1)]
            .iter()
            .all(|&b| b == 0xAA_u8));
        assert!(buffer[buffer_size * 5..(buffer_size * 6 - 1)]
            .iter()
            .all(|&b| b == 0x06_u8));

        delete_object(region.clone(), &bucket, &key).await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_unicode() {
        do_s3_multipart_test("s3 🧪 😱").await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_multipart_query_position() {
        // Verfies that the basesink::query handler is correctly handling Bytes -formatted
        // position queries according to the current position in the overall upload.
        init();

        let (region, bucket, key) = get_env_args("query-position");
        let uri = get_uri(&region, &bucket, &key);

        let mut h1 = gst_check::Harness::new_parse(&format!(
            "awss3sink name=\"sink\" uri=\"{uri}\" num-cached-parts=1"
        ));
        let h1el = h1
            .element()
            .unwrap()
            .dynamic_cast::<gst::Bin>()
            .unwrap()
            .by_name("sink")
            .unwrap();
        let part_size = h1el.property::<u64>("part-size");
        let pad = h1el.static_pad("sink").unwrap();

        // Start.
        h1.play();

        // Position should be 0 on start
        let mut position = pad.query_position::<gst::format::Bytes>();
        assert_eq!(0, u64::from(position.unwrap()));

        // Push stream start, segment, and 3 buffers
        let segment = gst::FormattedSegment::<gst::format::Bytes>::new();
        h1.push_event(gst::event::StreamStart::builder(&"test-stream").build());
        h1.push_event(gst::event::Segment::new(&segment));
        for i in 1..=3 as u8 {
            let buffer = make_buffer(&vec![i; part_size.try_into().unwrap()]);
            h1.push(buffer).unwrap();

            // Verify position is updating
            position = pad.query_position::<gst::format::Bytes>();
            assert_eq!(part_size * (i as u64), u64::from(position.unwrap()));
        }

        // Finish and clean up.
        h1.push_event(gst::event::Eos::new());
        delete_object(region.clone(), &bucket, &key).await;
    }

    #[test_with::env(AWS_ACCESS_KEY_ID)]
    #[test_with::env(AWS_SECRET_ACCESS_KEY)]
    #[tokio::test]
    async fn test_s3_multipart_query_seeking() {
        // Verfies the basesink::query handler is providing correct replies to Bytes -formatted
        // Seeking queries.  For now this test only verifies that it replies to seeking queries
        // with 0->max if caching is enabled since at this time it's unclear the right way to
        // represent a gap in seeking capability, for example head caching of 1 part but having
        // written into part 5, a seek request (via segment event) could go into either the
        // part cache or it could be within the locally-held buffer's range -- either of those
        // is logically correct, but there's only one pair of limits in this reply.  Therefore
        // this current implementation only replies 0->max if the cache is enabled and will
        // reply negatively in the event a segment goes out of range.
        init();

        let (region, bucket, key) = get_env_args("query-position");
        let uri = get_uri(&region, &bucket, &key);

        let h1 = gst_check::Harness::new_parse(&format!(
            "awss3sink name=\"sink\" uri=\"{uri}\" num-cached-parts=1"
        ));
        let h1el = h1
            .element()
            .unwrap()
            .dynamic_cast::<gst::Bin>()
            .unwrap()
            .by_name("sink")
            .unwrap();
        let pad = h1el.static_pad("sink").unwrap();

        let mut q = gst::query::Seeking::new(gst::Format::Bytes);
        assert!(pad.query(q.query_mut()));

        let (seekable, lower, upper) = q.result();
        assert!(seekable);
        assert_eq!(0, lower.value() as u64);
        assert_eq!(u64::MAX - 1, upper.value() as u64);
    }

    #[test_with::env(AWS_ACCESS_KEY_ID)]
    #[test_with::env(AWS_SECRET_ACCESS_KEY)]
    #[tokio::test]
    async fn test_s3_put_object_simple() {
        do_s3_putobject_test("s3-put-object-test", None, None, None, true).await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_put_object_whitespace() {
        do_s3_putobject_test("s3 put object test", None, None, None, true).await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_put_object_unicode() {
        do_s3_putobject_test("s3 put object 🧪 😱", None, None, None, true).await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_put_object_flush_buffers() {
        // Awkward threshold as we push 5 buffers
        do_s3_putobject_test("s3-put-object-test fbuf", Some(2), None, None, true).await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_put_object_flush_bytes() {
        // Awkward threshold as we push 14 bytes per buffer
        do_s3_putobject_test("s3-put-object-test fbytes", None, Some(30), None, true).await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_put_object_flush_time() {
        do_s3_putobject_test(
            "s3-put-object-test ftime",
            None,
            None,
            // Awkward threshold as we push each buffer with 200ms
            Some(gst::ClockTime::from_mseconds(300)),
            true,
        )
        .await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_put_object_on_eos() {
        // Disable all flush thresholds, so only EOS causes a flush
        do_s3_putobject_test(
            "s3-put-object-test eos",
            Some(0),
            Some(0),
            Some(gst::ClockTime::from_nseconds(0)),
            true,
        )
        .await;
    }

    #[ignore = "failing, needs investigation"]
    #[tokio::test]
    async fn test_s3_put_object_without_eos() {
        // Disable all flush thresholds, skip EOS, and cause a flush on error
        do_s3_putobject_test(
            "s3-put-object-test !eos",
            Some(0),
            Some(0),
            Some(gst::ClockTime::from_nseconds(0)),
            false,
        )
        .await;
    }
}
