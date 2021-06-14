require('dotenv').config();

const {createContext, CryptoFactory} = require('sawtooth-sdk/signing')

const axios = require('axios');
const context = createContext('secp256k1')
const privateKey = context.newRandomPrivateKey()
const signer = (new CryptoFactory(context)).newSigner(privateKey)
const crypto = require('crypto');
const hash = (x) =>
  crypto.createHash('sha512').update(x).digest('hex').toLowerCase()

const HOST = process.env.SAWTOOTH_REST;
console.log(`HOST: ${HOST}`);

const TP_FAMILY = 'tp1';
const TP_vERSION = '1.0';
const TP_NAMESPACE = hash(TP_FAMILY).substring(0, 6);

const address = (k) => 
  TP_NAMESPACE + hash(k).slice(-64)

const payload = {
  key: 'foo',
  value: 42
}

const payloadBytes = Buffer.from(JSON.stringify(payload), 'utf8');

const {createHash} = require('crypto')
const {protobuf} = require('sawtooth-sdk')

const transactionHeaderBytes = protobuf.TransactionHeader.encode({
  familyName: TP_FAMILY,
  familyVersion: TP_vERSION,
  inputs: [address(payload.key)],
  outputs: [address(payload.key)],
  signerPublicKey: signer.getPublicKey().asHex(),
  // In this example, we're signing the batch with the same private key,
  // but the batch can be signed by another party, in which case, the
  // public key will need to be associated with that key.
  batcherPublicKey: signer.getPublicKey().asHex(),
  // In this example, there are no dependencies.  This list should include
  // an previous transaction header signatures that must be applied for
  // this transaction to successfully commit.
  // For example,
  // dependencies: ['540a6803971d1880ec73a96cb97815a95d374cbad5d865925e5aa0432fcf1931539afe10310c122c5eaae15df61236079abbf4f258889359c4d175516934484a'],
  dependencies: [],
  payloadSha512: createHash('sha512').update(payloadBytes).digest('hex'),
  nonce:"hey4"
}).finish()

let signature = signer.sign(transactionHeaderBytes)

const transaction = protobuf.Transaction.create({
  header: transactionHeaderBytes,
  headerSignature: signature,
  payload: payloadBytes
})

//--------------------------------------
//Optional
//If sending to sign outside

const txnListBytes = protobuf.TransactionList.encode({transactions:[
  transaction
]}).finish()

//const txnBytes2 = transaction.finish()

let transactions = protobuf.TransactionList.decode(txnListBytes).transactions;

//----------------------------------------

//transactions = [transaction]

const batchHeaderBytes = protobuf.BatchHeader.encode({
  signerPublicKey: signer.getPublicKey().asHex(),
  transactionIds: transactions.map((txn) => txn.headerSignature),
}).finish()



signature = signer.sign(batchHeaderBytes)

const batch = protobuf.Batch.create({
  header: batchHeaderBytes,
  headerSignature: signature,
  transactions: transactions
})



const batchListBytes = protobuf.BatchList.encode({
  batches: [batch]
}).finish()



axios.post(`${HOST}/batches`, batchListBytes, {
  headers: {'Content-Type': 'application/octet-stream'}
})
  .then((response) => {
    console.log(response.data);
  })
  .catch((err)=>{
    console.log(err);
  });

