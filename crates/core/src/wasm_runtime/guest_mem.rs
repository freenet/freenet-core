//! Bounds-checked access to guest-controlled offsets in WASM linear memory.
//!
//! Every value a contract or delegate returns to the host is untrusted: the
//! buffer-builder offset from `__frnt__initiate_buffer`, the `start`,
//! `capacity`, `last_read` and `last_write` fields stored inside that builder,
//! and the `{ptr, kind, size}` result descriptor returned by each contract
//! export. The delegate `process` result descriptor is NOT checked here: it is
//! still read through stdlib's `DelegateInterfaceResult` and is being fixed
//! separately in freenet/freenet-core#6217.
//! `freenet-stdlib`'s host-side helpers (`BufferMut::from_ptr`,
//! `ContractInterfaceResult::from_raw` and the `unwrap_*` decoders) add those
//! offsets to the memory base without checking them against the memory size,
//! and panic on an unknown result kind. This module checks every such value
//! against the instance's current linear memory BEFORE the host reads or
//! writes through it, and decodes result descriptors without panicking.
//!
//! All offsets are relative to the start of linear memory. WASM memory is
//! little-endian regardless of the host, so fields are decoded with
//! `from_le_bytes` from copies of the guest bytes (no reference into guest
//! memory is created for an unchecked or unaligned location).

use freenet_stdlib::memory::WasmLinearMem;
use freenet_stdlib::memory::buf::BufferBuilder;

/// Size of `BufferBuilder` as laid out by the guest (`#[repr(C)]`:
/// `start: i64`, `capacity: u32`, padding, `last_read: i64`, `last_write: i64`).
const BUILDER_SIZE: u64 = 32;
const BUILDER_ALIGN: u64 = 8;
const BUILDER_START: usize = 0;
const BUILDER_CAPACITY: usize = 8;
const BUILDER_LAST_READ: usize = 16;
const BUILDER_LAST_WRITE: usize = 24;
/// The read/write position counters are `u32`s the guest allocates separately.
const COUNTER_SIZE: u64 = 4;
const COUNTER_ALIGN: u64 = 4;

/// Size of the result descriptor `ContractInterfaceResult` (`ptr: i64`,
/// `kind: i32`, `size: u32`), 16 bytes under `#[repr(C)]`.
const RESULT_DESCRIPTOR_SIZE: u64 = 16;

// The host hands these offsets to stdlib code that reinterprets guest memory as
// these types, so the layout constants above must match the host's view.
const _: () = assert!(std::mem::size_of::<BufferBuilder>() == BUILDER_SIZE as usize);
const _: () = assert!(
    std::mem::size_of::<freenet_stdlib::prelude::ContractInterfaceResult>()
        == RESULT_DESCRIPTOR_SIZE as usize
);

/// The `kind` discriminants of stdlib's private `ResultKind`, one per contract
/// export. A descriptor whose kind is not the one for the export that returned
/// it is refused. These match freenet-stdlib's `ResultKind` (0..=4); the
/// honest-contract tests fail if they drift, because every real contract
/// result would then be refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub(super) enum ContractResultKind {
    ValidateState = 0,
    UpdateState = 2,
    SummarizeState = 3,
    StateDelta = 4,
}

impl ContractResultKind {
    fn export(self) -> &'static str {
        match self {
            ContractResultKind::ValidateState => "validate_state",
            ContractResultKind::UpdateState => "update_state",
            ContractResultKind::SummarizeState => "summarize_state",
            ContractResultKind::StateDelta => "get_state_delta",
        }
    }
}

/// A guest-supplied offset or value that does not describe memory the guest owns.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum GuestMemoryError {
    #[error(
        "{what} at offset {offset} with length {len} lies outside linear memory of {mem_size} bytes"
    )]
    OutOfBounds {
        what: &'static str,
        offset: i64,
        len: u64,
        mem_size: u64,
    },

    #[error("{what} at offset {offset} is not {align}-byte aligned")]
    Misaligned {
        what: &'static str,
        offset: u64,
        align: u64,
    },

    #[error("buffer {a} overlaps buffer {b}")]
    Overlap { a: &'static str, b: &'static str },

    #[error("buffer {what} position {value} exceeds buffer capacity {capacity}")]
    PositionPastCapacity {
        what: &'static str,
        value: u32,
        capacity: u32,
    },

    #[error("{export} returned a result of kind {found}, expected kind {expected}")]
    WrongResultKind {
        export: &'static str,
        expected: i32,
        found: i32,
    },
}

/// Check that `[offset, offset + len)` lies inside `mem` and that `offset` is a
/// multiple of `align`. Returns the offset as `u64`.
fn check_range(
    mem: &WasmLinearMem,
    what: &'static str,
    offset: i64,
    len: u64,
    align: u64,
) -> Result<u64, GuestMemoryError> {
    let out_of_bounds = || GuestMemoryError::OutOfBounds {
        what,
        offset,
        len,
        mem_size: mem.size,
    };
    let start = u64::try_from(offset).map_err(|_| out_of_bounds())?;
    let end = start.checked_add(len).ok_or_else(out_of_bounds)?;
    if end > mem.size {
        return Err(out_of_bounds());
    }
    if start % align != 0 {
        return Err(GuestMemoryError::Misaligned {
            what,
            offset: start,
            align,
        });
    }
    Ok(start)
}

/// Copy `N` bytes at `offset` out of linear memory.
///
/// # Safety
/// `mem` must describe the instance's live linear memory, and
/// `[offset, offset + N)` must have been checked by [`check_range`] against it.
unsafe fn read_array<const N: usize>(mem: &WasmLinearMem, offset: u64) -> [u8; N] {
    let mut out = [0u8; N];
    // SAFETY: the caller guarantees the source range is inside linear memory;
    // `out` is a distinct host stack buffer, so the ranges do not overlap.
    unsafe {
        std::ptr::copy_nonoverlapping(mem.start_ptr.add(offset as usize), out.as_mut_ptr(), N);
    }
    out
}

fn le_i64(bytes: &[u8], at: usize) -> i64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    i64::from_le_bytes(b)
}

fn le_u32(bytes: &[u8], at: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(b)
}

fn le_i32(bytes: &[u8], at: usize) -> i32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[at..at + 4]);
    i32::from_le_bytes(b)
}

/// A guest `BufferBuilder` whose every field has been checked against linear
/// memory. Offsets are relative to the start of linear memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CheckedBuilder {
    pub start: u64,
    pub capacity: u32,
    pub last_read: u64,
    pub last_write: u64,
}

fn overlaps(a: (u64, u64), b: (u64, u64)) -> bool {
    // Half-open ranges [start, end); an empty range overlaps nothing.
    a.0 < a.1 && b.0 < b.1 && a.0 < b.1 && b.0 < a.1
}

/// Validate the guest `BufferBuilder` at `builder_offset` against `mem`.
///
/// Checks, in order: the builder struct itself is inside memory and 8-byte
/// aligned; the data region `[start, start + capacity)` is inside memory; both
/// position counters are inside memory and 4-byte aligned; the builder, the
/// two counters and the data region are pairwise disjoint (stdlib creates
/// `&mut` references to each, so overlap would alias); and both counters'
/// current values are at most `capacity` (stdlib computes
/// `capacity - last_write` unchecked).
///
/// Every honest builder produced by any freenet-stdlib release satisfies all
/// of these: it is a `Box<BufferBuilder>`, two `Box<u32>`s and a
/// `Vec<u8>::with_capacity` allocation, with both counters starting at zero.
pub(super) fn check_buffer_builder(
    mem: &WasmLinearMem,
    builder_offset: i64,
) -> Result<CheckedBuilder, GuestMemoryError> {
    let builder = check_range(
        mem,
        "buffer builder",
        builder_offset,
        BUILDER_SIZE,
        BUILDER_ALIGN,
    )?;
    // SAFETY: range checked just above against this same `mem`.
    let raw: [u8; BUILDER_SIZE as usize] = unsafe { read_array(mem, builder) };
    let start_field = le_i64(&raw, BUILDER_START);
    let capacity = le_u32(&raw, BUILDER_CAPACITY);
    let read_field = le_i64(&raw, BUILDER_LAST_READ);
    let write_field = le_i64(&raw, BUILDER_LAST_WRITE);

    let start = check_range(mem, "buffer data", start_field, u64::from(capacity), 1)?;
    let last_read = check_range(
        mem,
        "buffer read position",
        read_field,
        COUNTER_SIZE,
        COUNTER_ALIGN,
    )?;
    let last_write = check_range(
        mem,
        "buffer write position",
        write_field,
        COUNTER_SIZE,
        COUNTER_ALIGN,
    )?;

    let regions: [(&'static str, (u64, u64)); 4] = [
        ("builder", (builder, builder + BUILDER_SIZE)),
        ("read position", (last_read, last_read + COUNTER_SIZE)),
        ("write position", (last_write, last_write + COUNTER_SIZE)),
        ("data", (start, start + u64::from(capacity))),
    ];
    for (i, (a_name, a)) in regions.iter().enumerate() {
        for (b_name, b) in &regions[i + 1..] {
            if overlaps(*a, *b) {
                return Err(GuestMemoryError::Overlap {
                    a: a_name,
                    b: b_name,
                });
            }
        }
    }

    // SAFETY: the read counter range was checked above against this same `mem`.
    let read_value = u32::from_le_bytes(unsafe { read_array(mem, last_read) });
    // SAFETY: the write counter range was checked above against this same `mem`.
    let write_value = u32::from_le_bytes(unsafe { read_array(mem, last_write) });
    for (what, value) in [("read", read_value), ("write", write_value)] {
        if value > capacity {
            return Err(GuestMemoryError::PositionPastCapacity {
                what,
                value,
                capacity,
            });
        }
    }

    Ok(CheckedBuilder {
        start,
        capacity,
        last_read,
        last_write,
    })
}

/// Return the serialized payload a contract export's result descriptor points
/// at, after checking the descriptor location, its `kind` against `expected`,
/// and the payload range against `mem`.
///
/// # Safety
/// `mem` must describe the instance's CURRENT live linear memory (fetched after
/// the guest call returned, since the guest may have grown and so moved it),
/// and no guest code may run while the returned slice is alive.
pub(super) unsafe fn contract_result_payload(
    mem: &WasmLinearMem,
    descriptor_offset: i64,
    expected: ContractResultKind,
) -> Result<&[u8], GuestMemoryError> {
    let what = "result descriptor";
    let at = check_range(mem, what, descriptor_offset, RESULT_DESCRIPTOR_SIZE, 1)?;
    // SAFETY: range checked just above against this same `mem`.
    let raw: [u8; RESULT_DESCRIPTOR_SIZE as usize] = unsafe { read_array(mem, at) };
    let ptr = le_i64(&raw, 0);
    let kind = le_i32(&raw, 8);
    let size = le_u32(&raw, 12);
    if kind != expected as i32 {
        return Err(GuestMemoryError::WrongResultKind {
            export: expected.export(),
            expected: expected as i32,
            found: kind,
        });
    }
    // SAFETY: forwarded from this function's contract.
    unsafe { payload(mem, ptr, size) }
}

/// # Safety
/// Same as [`contract_result_payload`].
unsafe fn payload(mem: &WasmLinearMem, ptr: i64, size: u32) -> Result<&[u8], GuestMemoryError> {
    let start = check_range(mem, "result payload", ptr, u64::from(size), 1)?;
    // SAFETY: the range is inside linear memory (checked above), and the
    // caller guarantees no guest code runs (so nothing mutates or moves the
    // memory) while the slice is alive.
    Ok(unsafe { std::slice::from_raw_parts(mem.start_ptr.add(start as usize), size as usize) })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake linear memory backed by a host buffer, aligned to 8 like a real
    /// WASM memory base (which is page-aligned).
    struct FakeMem {
        buf: Vec<u64>,
    }

    impl FakeMem {
        fn new(bytes: usize) -> Self {
            Self {
                buf: vec![0u64; bytes.div_ceil(8)],
            }
        }
        fn bytes_mut(&mut self) -> &mut [u8] {
            let len = self.buf.len() * 8;
            // SAFETY: a `[u64]` buffer reinterpreted as bytes of the same length.
            unsafe { std::slice::from_raw_parts_mut(self.buf.as_mut_ptr().cast::<u8>(), len) }
        }
        fn put_i64(&mut self, at: usize, v: i64) {
            self.bytes_mut()[at..at + 8].copy_from_slice(&v.to_le_bytes());
        }
        fn put_u32(&mut self, at: usize, v: u32) {
            self.bytes_mut()[at..at + 4].copy_from_slice(&v.to_le_bytes());
        }
        fn put_i32(&mut self, at: usize, v: i32) {
            self.bytes_mut()[at..at + 4].copy_from_slice(&v.to_le_bytes());
        }
        fn mem(&self) -> WasmLinearMem {
            // SAFETY: describes the owned buffer, which outlives each use.
            unsafe { WasmLinearMem::new(self.buf.as_ptr().cast::<u8>(), self.buf.len() as u64 * 8) }
        }
        /// Lay out an honest builder at `b`: counters at b+32/b+40, data at b+48.
        fn honest_builder(&mut self, b: usize, capacity: u32) {
            self.put_i64(b, (b + 48) as i64);
            self.put_u32(b + 8, capacity);
            self.put_i64(b + 16, (b + 32) as i64);
            self.put_i64(b + 24, (b + 40) as i64);
        }
    }

    /// Safe wrappers for the tests: the fake memory is a live host buffer and
    /// no guest code exists, so the `unsafe` contract holds trivially.
    fn contract_payload(
        m: &FakeMem,
        desc: i64,
        kind: ContractResultKind,
    ) -> Result<Vec<u8>, GuestMemoryError> {
        let mem = m.mem();
        // SAFETY: `mem` describes `m`'s live buffer; nothing mutates it while
        // the slice is borrowed (it is copied out immediately).
        unsafe { contract_result_payload(&mem, desc, kind) }.map(<[u8]>::to_vec)
    }

    #[test]
    fn honest_builder_is_accepted() {
        let mut m = FakeMem::new(4096);
        m.honest_builder(1024, 100);
        let checked = check_buffer_builder(&m.mem(), 1024).unwrap();
        assert_eq!(
            checked,
            CheckedBuilder {
                start: 1072,
                capacity: 100,
                last_read: 1056,
                last_write: 1064,
            }
        );
    }

    #[test]
    fn zero_capacity_builder_with_dangling_start_is_accepted() {
        // `Vec::with_capacity(0)` yields a dangling, non-null start (1 for u8).
        let mut m = FakeMem::new(4096);
        m.honest_builder(1024, 0);
        m.put_i64(1024, 1);
        assert!(check_buffer_builder(&m.mem(), 1024).is_ok());
    }

    #[test]
    fn builder_offset_outside_memory_is_refused() {
        let m = FakeMem::new(4096);
        for offset in [-8, 4096, 4096 - 24, i64::MAX - 7, 0x7fff_ffff_0000] {
            assert!(
                matches!(
                    check_buffer_builder(&m.mem(), offset),
                    Err(GuestMemoryError::OutOfBounds { .. })
                ),
                "offset {offset}"
            );
        }
    }

    #[test]
    fn misaligned_builder_is_refused() {
        let mut m = FakeMem::new(4096);
        m.honest_builder(1024, 8);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1028),
            Err(GuestMemoryError::Misaligned { .. })
        ));
    }

    #[test]
    fn data_region_past_end_of_memory_is_refused() {
        let mut m = FakeMem::new(4096);
        m.honest_builder(1024, 8);
        m.put_i64(1024, 0x7fff_ffff_0000);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::OutOfBounds {
                what: "buffer data",
                ..
            })
        ));
        // In bounds start, capacity running past the end.
        m.honest_builder(1024, 4096);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::OutOfBounds {
                what: "buffer data",
                ..
            })
        ));
        // Negative start.
        m.honest_builder(1024, 8);
        m.put_i64(1024, -4096);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::OutOfBounds {
                what: "buffer data",
                ..
            })
        ));
    }

    #[test]
    fn counter_outside_memory_or_misaligned_is_refused() {
        let mut m = FakeMem::new(4096);
        m.honest_builder(1024, 8);
        m.put_i64(1024 + 16, 4094);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::OutOfBounds { .. })
        ));
        m.honest_builder(1024, 8);
        m.put_i64(1024 + 24, 1 << 40);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::OutOfBounds { .. })
        ));
        m.honest_builder(1024, 8);
        m.put_i64(1024 + 24, 1066);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::Misaligned { .. })
        ));
    }

    #[test]
    fn overlapping_regions_are_refused() {
        let mut m = FakeMem::new(4096);
        // Write counter inside the data region.
        m.honest_builder(1024, 64);
        m.put_i64(1024 + 24, 1080);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::Overlap { .. })
        ));
        // Data region covering the builder itself.
        m.honest_builder(1024, 64);
        m.put_i64(1024, 1000);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::Overlap { .. })
        ));
        // Both counters at the same address.
        m.honest_builder(1024, 64);
        m.put_i64(1024 + 24, 1056);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::Overlap { .. })
        ));
    }

    #[test]
    fn position_past_capacity_is_refused() {
        let mut m = FakeMem::new(4096);
        m.honest_builder(1024, 16);
        m.put_u32(1064, 17);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::PositionPastCapacity { what: "write", .. })
        ));
        m.put_u32(1064, 16);
        assert!(check_buffer_builder(&m.mem(), 1024).is_ok());
        m.put_u32(1056, u32::MAX);
        assert!(matches!(
            check_buffer_builder(&m.mem(), 1024),
            Err(GuestMemoryError::PositionPastCapacity { what: "read", .. })
        ));
    }

    fn descriptor(m: &mut FakeMem, at: usize, ptr: i64, kind: i32, size: u32) {
        m.put_i64(at, ptr);
        m.put_i32(at + 8, kind);
        m.put_u32(at + 12, size);
    }

    #[test]
    fn contract_result_with_matching_kind_returns_payload() {
        let mut m = FakeMem::new(4096);
        descriptor(
            &mut m,
            512,
            600,
            ContractResultKind::SummarizeState as i32,
            5,
        );
        m.bytes_mut()[600..605].copy_from_slice(&[9, 8, 7, 6, 5]);
        assert_eq!(
            contract_payload(&m, 512, ContractResultKind::SummarizeState).unwrap(),
            vec![9, 8, 7, 6, 5]
        );
    }

    #[test]
    fn contract_result_kind_mismatch_is_refused() {
        let mut m = FakeMem::new(4096);
        for kind in [7, -1, i32::MAX, 1, 2] {
            descriptor(&mut m, 512, 600, kind, 0);
            let r = contract_payload(&m, 512, ContractResultKind::ValidateState);
            assert!(
                matches!(r, Err(GuestMemoryError::WrongResultKind { found, .. }) if found == kind),
                "kind {kind}"
            );
        }
    }

    #[test]
    fn contract_result_out_of_bounds_is_refused() {
        let mut m = FakeMem::new(4096);
        for desc in [-16, 4096 - 8, i64::MAX] {
            let r = contract_payload(&m, desc, ContractResultKind::ValidateState);
            assert!(
                matches!(r, Err(GuestMemoryError::OutOfBounds { .. })),
                "desc {desc}"
            );
        }
        for (ptr, size) in [
            (0x7fff_ffff_0000i64, 8u32),
            (-1, 1),
            (4000, 200),
            (0, u32::MAX),
        ] {
            descriptor(&mut m, 512, ptr, 0, size);
            let r = contract_payload(&m, 512, ContractResultKind::ValidateState);
            assert!(
                matches!(
                    r,
                    Err(GuestMemoryError::OutOfBounds {
                        what: "result payload",
                        ..
                    })
                ),
                "ptr {ptr} size {size}"
            );
        }
    }
}
