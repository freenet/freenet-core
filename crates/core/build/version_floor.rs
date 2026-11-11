//! Single definition of which handshake floors the wire format can carry,
//! shared by `build.rs` (rejects bad builds) and `version_cmp::encode_wire`
//! (the encoder), so the two cannot drift. See `version_cmp.rs` for the layout.

/// `Ok(())` if `floor` is a valid min-compatible version for `version`.
///
/// A floor in the same major.minor series is always encodable (wire format 1).
/// A floor in a different series uses wire format 2, which needs the floor
/// major at most one below ours, minor < 64 and patch < 256.
pub const fn check_floor(version: (u8, u8, u16), floor: (u8, u8, u16)) -> Result<(), &'static str> {
    let (major, minor, patch) = version;
    let (f_major, f_minor, f_patch) = floor;
    if f_major > major
        || (f_major == major && f_minor > minor)
        || (f_major == major && f_minor == minor && f_patch > patch)
    {
        return Err("min_compatible must not exceed the version");
    }
    if f_major == major && f_minor == minor {
        return Ok(());
    }
    if major - f_major > 1 || f_minor > 63 || f_patch > 255 {
        return Err(
            "cross-series min_compatible must have a major at most one below the version, \
             minor < 64 and patch < 256",
        );
    }
    Ok(())
}
