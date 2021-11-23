# Karma

A currency that can be acquired by making a donation to Freenet, or by
providing resources to the network.

Karma is measured in coins. Coins can be earned by making a donation to Freenet.
A coin has a fixed value and one owner at a time.


# Vaults

When coin is minted it can specify one or more "vaults". These are centralized
archives of coin transactions that can be designated by the coin as authorative.
This archive can be used to verify coin transactions.

More sophisticated approaches with multiple vaults and rules for when they
disagree are also possible.

# Wallet

A wallet consists of a cryptographic public/private keypair, where the private key
is known only to the wallet owner.

The value of a wallet is a list of transactions which can be appended to.

## Donation

A record that the wallet owner made a donation to Freenet, the record must be
signed by Freenet and contains:

* The amount of the donation
* The date of the donation
* The public key of the wallet owner

## Feedback

Node A offers to do work for Node B, B agrees and A sends several signed feedback
records containing:

* The amount and nature of the work that will be done
* a boolean indicator of whether the work was done

Node B is given versions of the feedback where the boolean indicator is true or false, 
and is free to choose which to add to the wallet based on A's performace.

