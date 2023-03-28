# Blind Attestations

## Overview

A mechanism to attest the owner of a "target" contract performed some action,
while preserving the owner's anonymity using a [blind
signature](https://en.wikipedia.org/wiki/Blind_signature).

A typical use would be for the Freenet non-profit to attest that the owner of a
particular contract made a donation to project. The owner can then use this
attestation to prove they made a donation, without revealing their identity to
Freenet or anyone else.

This contract could then be thought to have a value of the donation amount,
this could then serve as collateral to secure a transaction with a
counterparty, such as a purchase or a loan.

To do this, the contract would allow the contract owner to temporarily give the
counterparty the ability to "disable" the contract for a mutually agreed period
of time. The parties then conduct their transaction. If the counterparty is
dissatisfied with the transaction then they can disable the contract as
punishment, during which time it cannot be used.

## Attestation

```rust
let contract_key = // The contract which we want Freenet to attest to
let (blinded_attestation_request, blind_key) = BlindAttestationRequest::blind(
    &mut rng,
    &contract_key,
);
```

The contract owner then sends the blinded attestation request to Freenet:

```rust
// URL is https://freenet.org/attestation?blinded_contract_key=4F6oPq...
open_in_browser(&blinded_attestation_request.to_url());
```

The user then follows the instructions on freenet.org to complete the donation.
Once the donation is complete, freenet.org signs the blinded_contract_key and
sends the attestation response through a response contract in Freenet. This
may also be sent via a browser redirect to the application.

The attestation consists of:

```rust
struct Attestation {
    pub signature : Signature,
    pub authorization : Authorization,
    pub authorization_sig : Signature,

    /// 
    fn is_valid(&self) -> Result<Authorization, String> {
        if (!signature.verify(&authorization.pubkey, &self.target)) {
            return Err("The target's signature is invalid");
        }

        if (!authorization_sig.verify(&freenet_public_key, &self.authorization)) {
            return Err("The authorization's signature is invalid");
        }

        Ok(self.authorization)
    }
}

enum Authorization {
    FreenetDonation(pubkey : PublicKey, amount_range : (Money, Money), time_range : (Timestamp, Timestamp)),
}

enum Target {
    Contract(ContractKey),
}
```
