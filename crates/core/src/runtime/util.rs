use rand::{rngs::ThreadRng, Rng};

#[inline]
pub fn generate_random_bytes(output: &mut [u8]) {
    let mut rng = ThreadRng::default();
    for element in output {
        *element = rng.gen();
    }
}
