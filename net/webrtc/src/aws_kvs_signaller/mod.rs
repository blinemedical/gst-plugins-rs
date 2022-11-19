// SPDX-License-Identifier: MPL-2.0

use crate::signaller::Signallable;
use gst::glib;

mod imp;
mod protocol;

glib::wrapper! {
    pub struct AwsKvsSignaller(ObjectSubclass<imp::Signaller>) @implements Signallable;
}

unsafe impl Send for AwsKvsSignaller {}
unsafe impl Sync for AwsKvsSignaller {}

impl Default for AwsKvsSignaller {
    fn default() -> Self {
        glib::Object::new()
    }
}
