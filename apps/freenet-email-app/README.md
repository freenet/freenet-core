# Inbox Contract

High-level functionality:

- Senders need to be able to add messages to the inbox contract state provided that they're accompanied by a token assignment where the assignment must be the public key of the inbox. Inbox should use related contracts mechanism to verify that the assignment is present in the token generator contract state and meets other criteria (eg. tier).
  - It's possible that the token assignment should include both the inbox public key and something tying it to the specific message being sent, perhaps a hash of the entire message, for example. This would be more robust than disallowing multiple messages with same assignment.
- Inbox owner needs to be able to delete messages once they're read, eg. owner sends signed update message to contract saying: "delete messages up to and including message id X".
