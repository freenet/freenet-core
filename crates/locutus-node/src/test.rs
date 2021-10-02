use rand::Rng;

// Would be nice to use const generics for this but there is a bound for the allowed
// array size parameter which cannot be expressed with current version of const generics.
// So using this macro hack.
macro_rules! rnd_bytes {
    ($size:tt -> $name:tt) => {
        #[inline]
        pub(crate) fn $name() -> [u8; $size] {
            let mut rng = rand::thread_rng();
            let mut rnd_bytes = [0u8; $size];
            rng.fill(&mut rnd_bytes);
            rnd_bytes
        }
    };
}

rnd_bytes!(128 -> random_bytes_128);
rnd_bytes!(1024 -> random_bytes_1024);
