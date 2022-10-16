//! Memory buffers to interact with the WASM contracts.

use crate::prelude::WasmLinearMem;

#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct BufferBuilder {
    start: i64,
    capacity: u32,
    last_read: i64,
    last_write: i64,
}

impl BufferBuilder {
    /// Return the buffer capacity.
    pub fn capacity(&self) -> usize {
        self.capacity as _
    }

    pub fn written(&self, mem: Option<&WasmLinearMem>) -> usize {
        unsafe {
            let ptr = if let Some(mem) = mem {
                compute_ptr(self.last_write as *mut u32, mem)
            } else {
                self.last_write as *mut u32
            };
            *ptr as usize
        }
    }

    /// Returns the first byte of buffer.
    pub fn start(&self) -> *mut u8 {
        self.start as _
    }

    /// # Safety
    /// Requires that there are no living references to the current
    /// underlying buffer or will trigger UB
    pub unsafe fn update_buffer(&mut self, data: Vec<u8>) {
        let read_ptr = Box::leak(Box::from_raw(self.last_read as *mut u32));
        let write_ptr = Box::leak(Box::from_raw(self.last_write as *mut u32));

        // drop previous buffer
        let prev = Vec::from_raw_parts(self.start as *mut u8, *write_ptr as usize, self.capacity());
        std::mem::drop(prev);

        // write the new buffer information
        let new_ptr = data.as_ptr();
        self.start = new_ptr as i64;
        self.capacity = data.capacity() as _;
        *read_ptr = 0;
        *write_ptr = data.len().saturating_sub(1) as _; // []
        std::mem::forget(data);
    }

    /// Returns a wrapped raw pointer to the buffer builder.
    pub fn to_ptr(self) -> *mut BufferBuilder {
        Box::into_raw(Box::new(self))
    }
}

/// Type of buffer errors.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("insufficient memory, needed {req} bytes but had {free} bytes")]
    InsufficientMemory { req: usize, free: usize },
}

/// Represents a mutable buffer in the wasm module.
#[derive(Debug)]
pub struct BufferMut<'instance> {
    buffer: &'instance mut [u8],
    /// stores the last read in the buffer
    read_ptr: &'instance u32,
    /// stores the last write in the buffer
    write_ptr: &'instance mut u32,
    /// A pointer to the underlying builder
    builder_ptr: *mut BufferBuilder,
    /// Linear memory pointer and size in bytes
    mem: WasmLinearMem,
}

impl<'instance> BufferMut<'instance> {
    /// Tries to write data into the buffer, after any unread bytes.
    pub fn write<T>(&mut self, obj: T) -> Result<(), Error>
    where
        T: AsRef<[u8]>,
    {
        let obj = obj.as_ref();
        if obj.len() > self.buffer.len() {
            return Err(Error::InsufficientMemory {
                req: obj.len(),
                free: self.buffer.len(),
            });
        }
        let mut last_write = (*self.write_ptr) as usize;
        let free_right = self.buffer.len() - last_write;
        if obj.len() <= free_right {
            let copy_to = &mut self.buffer[last_write..last_write + obj.len()];
            copy_to.copy_from_slice(obj);
            last_write += obj.len();
            *self.write_ptr = last_write as u32;
            Ok(())
        } else {
            Err(Error::InsufficientMemory {
                req: obj.len(),
                free: free_right,
            })
        }
    }

    /// Read bytes specified number of bytes from the buffer.
    ///
    /// Always read from the beginning.
    pub fn read_bytes(&self, len: usize) -> &[u8] {
        let next_offset = *self.read_ptr as usize;
        // don't update the read ptr
        &self.buffer[next_offset..next_offset + len]
    }

    /// Give ownership of the buffer back to the guest.
    pub fn shared(self) -> Buffer<'instance> {
        let BufferMut {
            builder_ptr, mem, ..
        } = self;
        let BuilderInfo {
            buffer,
            read_ptr,
            write_ptr,
            ..
        } = from_raw_builder(builder_ptr, mem);
        Buffer {
            buffer,
            read_ptr,
            write_ptr,
            builder_ptr,
            mem,
        }
    }

    /// Return the buffer capacity.
    pub fn size(&self) -> usize {
        unsafe {
            let p = &*compute_ptr(self.builder_ptr, &self.mem);
            p.capacity as _
        }
    }

    #[doc(hidden)]
    /// # Safety
    /// The pointer passed come from a previous call to `initiate_buffer` exported function from the contract.
    pub unsafe fn from_ptr(
        builder_ptr: *mut BufferBuilder,
        linear_mem_space: WasmLinearMem,
    ) -> Self {
        let BuilderInfo {
            buffer,
            read_ptr,
            write_ptr,
        } = from_raw_builder(builder_ptr, linear_mem_space);
        BufferMut {
            buffer,
            read_ptr,
            write_ptr,
            builder_ptr,
            mem: linear_mem_space,
        }
    }

    /// A pointer to the linear memory address.
    pub fn ptr(&self) -> *mut BufferBuilder {
        self.builder_ptr
    }
}

#[inline(always)]
pub(crate) fn compute_ptr<T>(ptr: *mut T, linear_mem_space: &WasmLinearMem) -> *mut T {
    let mem_start_ptr = linear_mem_space.start_ptr;
    (mem_start_ptr as isize + ptr as isize) as _
}

struct BuilderInfo<'instance> {
    buffer: &'instance mut [u8],
    read_ptr: &'instance mut u32,
    write_ptr: &'instance mut u32,
}

fn from_raw_builder<'a>(builder_ptr: *mut BufferBuilder, mem: WasmLinearMem) -> BuilderInfo<'a> {
    unsafe {
        #[cfg(feature = "trace")]
        {
            let contract_mem = std::slice::from_raw_parts(mem.start_ptr, mem.size as usize);
            log::trace!(
                "*mut BufferBuilder <- offset: {}; in mem: {:?}",
                builder_ptr as usize,
                &contract_mem[builder_ptr as usize
                    ..builder_ptr as usize + std::mem::size_of::<BufferBuilder>()]
            );
            // use std::{fs::File, io::Write};
            // let mut f = File::create(std::env::temp_dir().join("dump.mem")).unwrap();
            // f.write_all(contract_mem).unwrap();
        }

        let builder_ptr = compute_ptr(builder_ptr, &mem);
        let buf_builder: &'static mut BufferBuilder = Box::leak(Box::from_raw(builder_ptr));
        #[cfg(feature = "trace")]
        {
            log::trace!("buf builder from FFI: {buf_builder:?}");
        }

        let read_ptr = Box::leak(Box::from_raw(compute_ptr(
            buf_builder.last_read as *mut u32,
            &mem,
        )));
        let write_ptr = Box::leak(Box::from_raw(compute_ptr(
            buf_builder.last_write as *mut u32,
            &mem,
        )));
        let buffer_ptr = compute_ptr(buf_builder.start as *mut u8, &mem);
        let buffer =
            &mut *std::ptr::slice_from_raw_parts_mut(buffer_ptr, buf_builder.capacity as usize);
        BuilderInfo {
            buffer,
            read_ptr,
            write_ptr,
        }
    }
}

#[derive(Debug)]
/// Represents a buffer in the wasm module.
pub struct Buffer<'instance> {
    buffer: &'instance mut [u8],
    /// stores the last read in the buffer
    read_ptr: &'instance mut u32,
    write_ptr: &'instance u32,
    builder_ptr: *mut BufferBuilder,
    mem: WasmLinearMem,
}

impl<'instance> Buffer<'instance> {
    /// # Safety
    /// In order for this to be a safe T must be properly aligned and cannot re-use the buffer
    /// trying to read the same memory region again (that would create more than one copy to
    /// the same underlying data and break aliasing rules).
    pub unsafe fn read<T: Sized>(&mut self) -> T {
        let next_offset = *self.read_ptr as usize;
        let bytes = &self.buffer[next_offset..next_offset + std::mem::size_of::<T>()];
        let t = std::ptr::read(bytes.as_ptr() as *const T);
        *self.read_ptr += std::mem::size_of::<T>() as u32;
        t
    }

    /// Read bytes specified number of bytes from the buffer.
    pub fn read_bytes(&mut self, len: usize) -> &[u8] {
        let next_offset = *self.read_ptr as usize;
        *self.read_ptr += len as u32;
        &self.buffer[next_offset..next_offset + len]
    }

    /// Reads all the bytes from the buffer.
    pub fn read_all(&mut self) -> &[u8] {
        let next_offset = *self.read_ptr as usize;
        *self.read_ptr += self.buffer.len() as u32;
        &self.buffer[next_offset..=*self.write_ptr as usize]
    }

    /// Give ownership of the buffer back to the guest.
    ///
    /// # Safety
    /// Must guarantee that there are not underlying alive shared references.
    #[doc(hidden)]
    pub unsafe fn exclusive(self) -> BufferMut<'instance> {
        let Buffer {
            builder_ptr, mem, ..
        } = self;
        let BuilderInfo {
            buffer,
            read_ptr,
            write_ptr,
        } = from_raw_builder(builder_ptr, mem);
        BufferMut {
            buffer,
            read_ptr,
            write_ptr,
            builder_ptr,
            mem,
        }
    }
}

/// Returns the pointer to a new BufferBuilder.
///
/// This buffer leaks it's own memory and will only be freed by the runtime when a contract instance is dropped.
#[doc(hidden)]
#[no_mangle]
pub fn initiate_buffer(capacity: u32) -> i64 {
    let buf: Vec<u8> = Vec::with_capacity(capacity as usize);
    let start = buf.as_ptr() as i64;

    let last_read = Box::into_raw(Box::new(0u32));
    let last_write = Box::into_raw(Box::new(0u32));
    let buffer = Box::into_raw(Box::new(BufferBuilder {
        start,
        capacity,
        last_read: last_read as _,
        last_write: last_write as _,
    }));
    #[cfg(feature = "trace")]
    {
        log::trace!(
            "new buffer ptr: {:p} -> {} as i64 w/ cap: {capacity}",
            buf.as_ptr(),
            start
        );
        log::trace!(
            "last read ptr: {last_read:p} -> {} as i64",
            last_read as i64
        );
        log::trace!(
            "last write ptr: {last_write:p} -> {} as i64",
            last_write as i64
        );
        log::trace!("buffer ptr: {buffer:p} -> {} as i64", buffer as i64);
    }
    std::mem::forget(buf);
    buffer as i64
}

#[cfg(test)]
mod test {
    use super::*;
    use wasmer::{
        imports, namespace, wat2wasm, Cranelift, Function, Instance, Module, NativeFunc, Store,
        Universal,
    };
    use wasmer_wasi::WasiState;

    const TEST_MODULE: &str = r#"
        (module
            (func $initiate_buffer (import "locutus" "initiate_buffer") (param i32) (result i64))
            (memory $locutus_mem (export "memory") 20)
            (export "initiate_buffer" (func $initiate_buffer))
        )"#;

    fn build_test_mod() -> Result<(Store, Instance), Box<dyn std::error::Error>> {
        let wasm_bytes = wat2wasm(TEST_MODULE.as_bytes())?;
        let store = Store::new(&Universal::new(Cranelift::new()).engine());
        let module = Module::new(&store, wasm_bytes)?;

        let init_buf_fn = Function::new_native(&store, initiate_buffer);
        let imports = imports! {
            "locutus" => { "initiate_buffer" => init_buf_fn }
        };
        let instance = Instance::new(&module, &imports).unwrap();
        Ok((store, instance))
    }

    #[allow(dead_code)]
    fn build_test_mod_with_wasi() -> Result<(Store, Instance), Box<dyn std::error::Error>> {
        let wasm_bytes = wat2wasm(TEST_MODULE.as_bytes())?;
        let store = Store::new(&Universal::new(Cranelift::new()).engine());
        let module = Module::new(&store, wasm_bytes)?;

        let init_buf_fn = Function::new_native(&store, initiate_buffer);
        let funcs = namespace!("initiate_buffer" => init_buf_fn );
        let mut wasi_env = WasiState::new("locutus").finalize()?;
        let mut imports = wasi_env.import_object(&module)?;
        imports.register("locutus", funcs);

        let instance = Instance::new(&module, &imports).unwrap();
        Ok((store, instance))
    }

    fn init_buf(instance: &Instance, size: u32) -> *mut BufferBuilder {
        let initiate_buffer: NativeFunc<u32, i64> = instance
            .exports
            .get_native_function("initiate_buffer")
            .unwrap();
        initiate_buffer.call(size).unwrap() as *mut BufferBuilder
    }

    #[test]
    #[ignore]
    fn read_and_write() -> Result<(), Box<dyn std::error::Error>> {
        let (_store, instance) = build_test_mod()?;
        let mem = instance.exports.get_memory("memory")?;
        let linear_mem = WasmLinearMem {
            start_ptr: mem.data_ptr() as *const _,
            size: mem.data_size(),
        };

        let mut writer = unsafe { BufferMut::from_ptr(init_buf(&instance, 10), linear_mem) };
        writer.write(&[1u8, 2])?;
        let mut reader = writer.shared();
        let r: [u8; 2] = unsafe { reader.read() };
        assert_eq!(r, [1, 2]);

        let mut writer = unsafe { reader.exclusive() };
        writer.write(&[3u8, 4])?;
        let mut reader = writer.shared();
        let r: [u8; 2] = unsafe { reader.read() };
        assert_eq!(r, [3, 4]);
        Ok(())
    }

    #[test]
    #[ignore]
    fn read_and_write_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let (_store, instance) = build_test_mod()?;
        let mem = instance.exports.get_memory("memory")?;
        let linear_mem = WasmLinearMem {
            start_ptr: mem.data_ptr() as *const _,
            size: mem.data_size(),
        };

        let mut writer = unsafe { BufferMut::from_ptr(init_buf(&instance, 10), linear_mem) };
        writer.write(&[1u8, 2])?;
        let mut reader = writer.shared();
        let r = reader.read_bytes(2);
        assert_eq!(r, &[1, 2]);

        let mut writer = unsafe { reader.exclusive() };
        writer.write(&[3u8, 4])?;
        let mut reader = writer.shared();
        let r = reader.read_bytes(2);
        assert_eq!(r, &[3, 4]);
        Ok(())
    }

    #[test]
    #[ignore]
    fn update() -> Result<(), Box<dyn std::error::Error>> {
        let (_store, instance) = build_test_mod()?;
        let mem = instance.exports.get_memory("memory")?;
        let linear_mem = WasmLinearMem {
            start_ptr: mem.data_ptr() as *const _,
            size: mem.data_size(),
        };

        let ptr = {
            let mut writer = unsafe { BufferMut::from_ptr(init_buf(&instance, 10), linear_mem) };
            writer.write(&[1u8, 2])?;
            writer.ptr()
        };

        let writer = unsafe {
            let builder = &mut *ptr;
            builder.update_buffer(vec![3, 5, 7]);
            BufferMut::from_ptr(ptr, linear_mem)
        };
        let mut reader = writer.shared();
        assert_eq!(reader.read_all(), &[3, 5, 7]);

        Ok(())
    }
}
