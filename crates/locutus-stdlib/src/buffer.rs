#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct BufferBuilder {
    size: u32,
    start: i64,
    last_read: i64,
    last_write: i64,
    host_writer: i32,
}

impl BufferBuilder {
    pub fn size(&self) -> usize {
        self.size as _
    }

    pub fn start(&self) -> *const u8 {
        self.start as _
    }

    /// # Safety
    /// Requires that there are no living references to the current
    /// underlying buffer or will trigger UB
    pub unsafe fn update_buffer(&mut self, data: Vec<u8>) {
        // drop previous buffer
        let prev = Vec::from_raw_parts(self.start as *mut u8, self.size(), self.size());
        std::mem::drop(prev);
        let new_ptr = data.as_ptr() as i64;
        self.start = new_ptr;
        self.size = data.capacity() as _;

        // write the new buffer read/write pointers
        let read_ptr = Box::leak(Box::from_raw(self.last_read as *mut u32));
        *read_ptr = 0;
        let write_ptr = Box::leak(Box::from_raw(self.last_write as *mut u32));
        *write_ptr = data.len() as _;
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("insufficient memory, needed {req} bytes but had {free} bytes")]
    InsufficientMemory { req: usize, free: usize },
}

/// Represents a buffer in the wasm module.
#[derive(Debug)]
pub struct BufferMut<'instance> {
    buffer: &'instance mut [u8],
    /// stores the last read in the buffer
    read_ptr: &'instance mut u32,
    /// stores the last write in the buffer
    write_ptr: &'instance mut u32,
    /// A pointer to the underlying builder
    builder_ptr: *mut BufferBuilder,
    /// A pointer to the builder in the linear memory address space,
    /// in case it is different than the address set at the `builder_ptr`
    original_ptr: Option<*mut BufferBuilder>,
}

impl<'instance> BufferMut<'instance> {
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
        } else {
            todo!()
        }
        Ok(())
    }

    pub fn read_bytes(&self, len: usize) -> &[u8] {
        let next_offset = *self.read_ptr as usize;
        // don't update the read ptr
        &self.buffer[next_offset..next_offset + len]
    }

    /// Give ownership of the buffer back to the guest.
    pub fn flip_ownership(self) -> Buffer<'instance> {
        let BufferMut {
            buffer,
            read_ptr,
            write_ptr,
            builder_ptr,
            original_ptr,
        } = self;
        Buffer {
            buffer,
            read_ptr,
            write_ptr,
            builder_ptr,
            original_ptr,
        }
    }

    pub fn size(&self) -> usize {
        let p = unsafe { &*self.builder_ptr };
        p.size as usize
    }

    #[doc(hidden)]
    /// # Safety
    /// The pointer passed come from a previous call to `initiate_buffer` exported function from the contract.
    pub unsafe fn from_ptr(
        builder_ptr: *mut BufferBuilder,
        linear_mem_start: Option<*mut u8>,
    ) -> Self {
        if let Some(start_ptr) = linear_mem_start {
            let original_ptr = Some(builder_ptr);
            let builder_ptr = (builder_ptr as usize + start_ptr as usize) as *mut BufferBuilder;
            let buf_builder: &'static mut BufferBuilder = Box::leak(Box::from_raw(builder_ptr));
            let read_ptr = Box::leak(Box::from_raw(
                (buf_builder.last_read as usize + start_ptr as usize) as *mut u32,
            ));
            let write_ptr = Box::leak(Box::from_raw(
                (buf_builder.last_write as usize + start_ptr as usize) as *mut u32,
            ));
            let buffer = &mut *std::ptr::slice_from_raw_parts_mut(
                (buf_builder.start as usize + start_ptr as usize) as *mut u8,
                buf_builder.size as usize,
            );
            BufferMut {
                buffer,
                read_ptr,
                write_ptr,
                builder_ptr,
                original_ptr,
            }
        } else {
            let buf_builder: &'static mut BufferBuilder = Box::leak(Box::from_raw(builder_ptr));
            let read_ptr = Box::leak(Box::from_raw(buf_builder.last_read as *mut u32));
            let write_ptr = Box::leak(Box::from_raw(buf_builder.last_write as *mut u32));
            let buffer = &mut *std::ptr::slice_from_raw_parts_mut(
                buf_builder.start as *mut u8,
                buf_builder.size as usize,
            );
            BufferMut {
                buffer,
                read_ptr,
                write_ptr,
                builder_ptr,
                original_ptr: None,
            }
        }
    }

    /// A pointer to the linear memory address.
    pub fn ptr(&self) -> *mut BufferBuilder {
        self.original_ptr.unwrap_or(self.builder_ptr)
    }
}

/// Represents a buffer in the wasm module.
pub struct Buffer<'instance> {
    buffer: &'instance mut [u8],
    /// stores the last read in the buffer
    read_ptr: &'instance mut u32,
    /// stores the last write in the buffer
    write_ptr: &'instance mut u32,
    builder_ptr: *mut BufferBuilder,
    original_ptr: Option<*mut BufferBuilder>,
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

    pub fn read_bytes(&mut self, len: usize) -> &[u8] {
        let next_offset = *self.read_ptr as usize;
        *self.read_ptr += len as u32;
        &self.buffer[next_offset..next_offset + len]
    }

    /// Give ownership of the buffer back to the guest.
    pub fn flip_ownership(self) -> BufferMut<'instance> {
        let Buffer {
            buffer,
            read_ptr,
            write_ptr,
            builder_ptr,
            original_ptr,
        } = self;
        BufferMut {
            buffer,
            read_ptr,
            write_ptr,
            builder_ptr,
            original_ptr,
        }
    }
}

/// Returns the pointer to a new BufferBuilder
#[doc(hidden)]
#[no_mangle]
pub fn initiate_buffer(size: u32, host_writer: i32) -> i64 {
    let buf: Vec<u8> = Vec::with_capacity(size as usize);
    // eprintln!("allocated {size} bytes @ {:p}", buf.as_ptr());
    let start = buf.as_ptr() as i64;
    std::mem::forget(buf);

    let last_read = Box::into_raw(Box::new(0u32)) as i64;
    let last_write = Box::into_raw(Box::new(0u32)) as i64;
    let buffer = Box::into_raw(Box::new(BufferBuilder {
        start,
        size,
        last_read,
        last_write,
        host_writer,
    }));
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
            (func $initiate_buffer (import "locutus" "initiate_buffer") (param i32 i32) (result i64))
            (memory $locutus_mem (export "memory") 20)
            (export "initiate_buffer" (func $initiate_buffer))
        )"#;

    #[allow(dead_code)]
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
        let initiate_buffer: NativeFunc<(u32, i32), i64> = instance
            .exports
            .get_native_function("initiate_buffer")
            .unwrap();
        initiate_buffer.call(size, true as i32).unwrap() as *mut BufferBuilder
    }

    #[test]
    fn read_and_write() -> Result<(), Box<dyn std::error::Error>> {
        let (_store, instance) = build_test_mod()?;
        let _mem = instance.exports.get_memory("memory")?.data_ptr();
        let mut writer = unsafe { BufferMut::from_ptr(init_buf(&instance, 10), None) };
        writer.write(&[1u8, 2])?;
        let mut reader = writer.flip_ownership();
        let r: [u8; 2] = unsafe { reader.read() };
        assert_eq!(r, [1, 2]);

        let mut writer = reader.flip_ownership();
        writer.write(&[3u8, 4])?;
        let mut reader = writer.flip_ownership();
        let r: [u8; 2] = unsafe { reader.read() };
        assert_eq!(r, [3, 4]);

        Ok(())
    }

    #[test]
    fn read_and_write_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let (_store, instance) = build_test_mod()?;
        let _mem = instance.exports.get_memory("memory")?.data_ptr();
        let mut writer = unsafe { BufferMut::from_ptr(init_buf(&instance, 10), None) };
        writer.write(&[1u8, 2])?;
        let mut reader = writer.flip_ownership();
        let r = reader.read_bytes(2);
        assert_eq!(r, &[1, 2]);

        let mut writer = reader.flip_ownership();
        writer.write(&[3u8, 4])?;
        let mut reader = writer.flip_ownership();
        let r = reader.read_bytes(2);
        assert_eq!(r, &[3, 4]);

        Ok(())
    }
}
