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

    #[ignore = "failing, needs investigation"]
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
