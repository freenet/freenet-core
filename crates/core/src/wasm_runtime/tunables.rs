//! Custom WASM memory tunables for limiting memory usage.
//!
//! This module provides `LimitingTunables`, a wrapper around Wasmer's `BaseTunables`
//! that enforces a maximum memory limit on WASM instances. This prevents unbounded
//! virtual address space accumulation that can lead to OOM errors over time.
//!
//! See: https://github.com/freenet/freenet-core/issues/2774

use std::ptr::NonNull;
use wasmer::sys::vm::{
    MemoryError, MemoryStyle, TableStyle, VMMemory, VMMemoryDefinition, VMTable, VMTableDefinition,
};
pub use wasmer::sys::BaseTunables;
use wasmer::sys::Tunables;
use wasmer::{MemoryType, Pages, TableType};

/// Default maximum memory limit in WASM pages (64 KiB each).
/// 4096 pages = 256 MiB, which is sufficient for most contracts while
/// preventing unbounded growth.
pub const DEFAULT_MAX_MEMORY_PAGES: u32 = 4096;

/// A tunables wrapper that enforces a maximum memory limit on WASM instances.
///
/// This prevents the accumulation of large virtual address space reservations
/// that can occur when WASM instances don't specify a memory maximum. Without
/// a limit, each instance can reserve ~5GB of virtual address space for guard
/// pages, leading to eventual OOM errors.
///
/// # Example
///
/// ```ignore
/// let base = BaseTunables::for_target(&Target::default());
/// let tunables = LimitingTunables::new(base, Pages(4096)); // 256 MiB limit
/// engine.set_tunables(tunables);
/// ```
pub struct LimitingTunables<T: Tunables> {
    /// The maximum a linear memory is allowed to be (in WASM pages, 64 KiB each).
    limit: Pages,
    /// The base implementation we delegate all the logic to.
    base: T,
}

impl<T: Tunables> LimitingTunables<T> {
    /// Create a new `LimitingTunables` with the specified memory limit.
    ///
    /// # Arguments
    ///
    /// * `base` - The base tunables to delegate to
    /// * `limit` - Maximum memory in WASM pages (64 KiB each)
    pub fn new(base: T, limit: Pages) -> Self {
        Self { limit, base }
    }

    /// Adjust the memory type to have a maximum if it doesn't have one.
    fn adjust_memory(&self, requested: &MemoryType) -> MemoryType {
        let mut adjusted = *requested;
        if requested.maximum.is_none() {
            adjusted.maximum = Some(self.limit);
        }
        adjusted
    }

    /// Validate that the memory type doesn't exceed our limit.
    fn validate_memory(&self, ty: &MemoryType) -> Result<(), MemoryError> {
        if ty.minimum > self.limit {
            return Err(MemoryError::Generic(format!(
                "Minimum memory ({} pages) exceeds the allowed limit ({} pages)",
                ty.minimum.0, self.limit.0
            )));
        }

        if let Some(max) = ty.maximum {
            if max > self.limit {
                return Err(MemoryError::Generic(format!(
                    "Maximum memory ({} pages) exceeds the allowed limit ({} pages)",
                    max.0, self.limit.0
                )));
            }
        } else {
            return Err(MemoryError::Generic(
                "Maximum memory unset after adjustment".to_string(),
            ));
        }

        Ok(())
    }
}

impl<T: Tunables> Tunables for LimitingTunables<T> {
    fn memory_style(&self, memory: &MemoryType) -> MemoryStyle {
        let adjusted = self.adjust_memory(memory);
        self.base.memory_style(&adjusted)
    }

    fn table_style(&self, table: &TableType) -> TableStyle {
        self.base.table_style(table)
    }

    fn create_host_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
    ) -> Result<VMMemory, MemoryError> {
        let adjusted = self.adjust_memory(ty);
        self.validate_memory(&adjusted)?;
        self.base.create_host_memory(&adjusted, style)
    }

    unsafe fn create_vm_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
        vm_definition_location: NonNull<VMMemoryDefinition>,
    ) -> Result<VMMemory, MemoryError> {
        let adjusted = self.adjust_memory(ty);
        self.validate_memory(&adjusted)?;
        unsafe {
            self.base
                .create_vm_memory(&adjusted, style, vm_definition_location)
        }
    }

    fn create_host_table(&self, ty: &TableType, style: &TableStyle) -> Result<VMTable, String> {
        self.base.create_host_table(ty, style)
    }

    unsafe fn create_vm_table(
        &self,
        ty: &TableType,
        style: &TableStyle,
        vm_definition_location: NonNull<VMTableDefinition>,
    ) -> Result<VMTable, String> {
        unsafe { self.base.create_vm_table(ty, style, vm_definition_location) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adjust_memory_adds_maximum() {
        let base = BaseTunables {
            static_memory_bound: Pages(2048),
            static_memory_offset_guard_size: 128,
            dynamic_memory_offset_guard_size: 256,
        };
        let tunables = LimitingTunables::new(base, Pages(4096));

        // Memory without maximum should get one added
        let mem_type = MemoryType::new(10, None, false);
        let adjusted = tunables.adjust_memory(&mem_type);
        assert_eq!(adjusted.maximum, Some(Pages(4096)));

        // Memory with smaller maximum should keep it
        let mem_type = MemoryType::new(10, Some(100), false);
        let adjusted = tunables.adjust_memory(&mem_type);
        assert_eq!(adjusted.maximum, Some(Pages(100)));
    }

    #[test]
    fn test_validate_memory_rejects_too_large() {
        let base = BaseTunables {
            static_memory_bound: Pages(2048),
            static_memory_offset_guard_size: 128,
            dynamic_memory_offset_guard_size: 256,
        };
        let tunables = LimitingTunables::new(base, Pages(100));

        // Minimum exceeds limit
        let mem_type = MemoryType::new(200, Some(200), false);
        assert!(tunables.validate_memory(&mem_type).is_err());

        // Maximum exceeds limit
        let mem_type = MemoryType::new(10, Some(200), false);
        assert!(tunables.validate_memory(&mem_type).is_err());

        // Within limit should pass
        let mem_type = MemoryType::new(10, Some(50), false);
        assert!(tunables.validate_memory(&mem_type).is_ok());
    }
}
