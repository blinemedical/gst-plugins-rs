use glib::prelude::*;

mod device_provider;
pub mod ndi;
mod ndiaudiosrc;
pub mod ndisys;
mod ndivideosrc;
pub mod receiver;

use crate::ndi::*;
use crate::ndisys::*;
use crate::receiver::*;

use std::collections::HashMap;
use std::time;

use once_cell::sync::Lazy;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, glib::GEnum)]
#[repr(u32)]
#[genum(type_name = "GstNdiTimestampMode")]
pub enum TimestampMode {
    #[genum(name = "Receive Time", nick = "receive-time")]
    ReceiveTime = 0,
    #[genum(name = "NDI Timecode", nick = "timecode")]
    Timecode = 1,
    #[genum(name = "NDI Timestamp", nick = "timestamp")]
    Timestamp = 2,
}

fn plugin_init(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    if !ndi::initialize() {
        return Err(glib::glib_bool_error!("Cannot initialize NDI"));
    }

    ndivideosrc::register(plugin)?;
    ndiaudiosrc::register(plugin)?;
    device_provider::register(plugin)?;
    Ok(())
}

static DEFAULT_RECEIVER_NDI_NAME: Lazy<String> = Lazy::new(|| {
    format!(
        "GStreamer NDI Source {}-{}",
        env!("CARGO_PKG_VERSION"),
        env!("COMMIT_ID")
    )
});

#[cfg(feature = "reference-timestamps")]
static TIMECODE_CAPS: Lazy<gst::Caps> =
    Lazy::new(|| gst::Caps::new_simple("timestamp/x-ndi-timecode", &[]));
#[cfg(feature = "reference-timestamps")]
static TIMESTAMP_CAPS: Lazy<gst::Caps> =
    Lazy::new(|| gst::Caps::new_simple("timestamp/x-ndi-timestamp", &[]));

gst::gst_plugin_define!(
    ndi,
    env!("CARGO_PKG_DESCRIPTION"),
    plugin_init,
    concat!(env!("CARGO_PKG_VERSION"), "-", env!("COMMIT_ID")),
    "LGPL",
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_REPOSITORY"),
    env!("BUILD_REL_DATE")
);
