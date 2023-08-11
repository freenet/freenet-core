# Description

...

# Functioning

Some characteristics to take into account:

- Tokens are "generated" (or available) by the delegate all the time. Past slots may have been
  allocated or not, in which case they are stillf ree to use.

Example of how the AFT module can be used, in the context of a mailing application:

1. Alice (sender) has Bob (receiver) public key and wants to send an email to Bob.
2. Bob's inbox contract specifies the token assignment criteria (tier, recency, etc.).
   - Bob is able to change that setting and those setting must be accesible publically.
     The contract must keep track of when changes have been done to the criteria to not invalidate old messages.
     Messages should be indexed, and we can use that to keep track of what settings should apply to each message.
   - Tokens max allowed age is specified by the inbox contract of the receiver.
3. The AFT delegate verifies that Alice agrees with allocating tokens for Bob's messages (or all messages). After which the message is sent to Bob, with the allocated token attached.
4. Bob's inbox contract uses the related contract mechanism to verify that Alice's token assignment is present on Alice's token record to avoid double-spending.
5. The AFT generator contract and its signature match the message.
6. Bob periodically reads his inbox and clears his inbox contract once the messages are downloaded/read.
