import { EXTENSION_PNG } from '../helpers/constants';
import path from 'path';
import {
  createConfig,
  loadCandyProgram,
  loadWalletKey,
} from '../helpers/accounts';
import { PublicKey } from '@solana/web3.js';
import fs from 'fs';
import { BN } from '@project-serum/anchor';
import { loadCache, saveCache } from '../helpers/cache';
import log from 'loglevel';
import { awsUpload } from '../helpers/upload/aws';
import { arweaveUpload } from '../helpers/upload/arweave';
import { ipfsCreds, ipfsUpload } from '../helpers/upload/ipfs';
import { chunks } from '../helpers/various';

const BATCH_SIZE = { arweave: 5, ipfs: 1, aws: 1 };

export async function upload(
  files: string[],
  cacheName: string,
  env: string,
  keypair: string,
  totalNFTs: number,
  storage: string,
  retainAuthority: boolean,
  mutable: boolean,
  rpcUrl: string,
  ipfsCredentials: ipfsCreds,
  awsS3Bucket: string,
): Promise<boolean> {
  let uploadSuccessful = true;

  const savedContent = loadCache(cacheName, env);
  const cacheContent = savedContent || {};

  if (!cacheContent.program) {
    cacheContent.program = {};
  }

  let existingInCache = [];
  if (!cacheContent.items) {
    cacheContent.items = {};
  } else {
    existingInCache = Object.keys(cacheContent.items);
  }

  const seen = {};
  const newFiles = [];

  files.forEach(f => {
    if (!seen[f.replace(EXTENSION_PNG, '').split('/').pop()]) {
      seen[f.replace(EXTENSION_PNG, '').split('/').pop()] = true;
      newFiles.push(f);
    }
  });
  existingInCache.forEach(f => {
    if (!seen[f]) {
      seen[f] = true;
      newFiles.push(f + '.png');
    }
  });

  const images = newFiles.filter(val => path.extname(val) === EXTENSION_PNG);
  const SIZE = images.length;

  const walletKeyPair = loadWalletKey(keypair);
  const anchorProgram = await loadCandyProgram(walletKeyPair, env, rpcUrl);

  let config = cacheContent.program.config
    ? new PublicKey(cacheContent.program.config)
    : undefined;

  let currentBatchStartIndex = 0;
  while (currentBatchStartIndex < SIZE) {
    console.log(
      `Upload for : ${currentBatchStartIndex} to ${Math.min(
        currentBatchStartIndex + BATCH_SIZE[storage],
        SIZE,
      )}`,
    );
    const promises = [];
    for (
      let i = currentBatchStartIndex;
      i < Math.min(currentBatchStartIndex + BATCH_SIZE[storage], SIZE);
      i++
    ) {
      promises.push(
        upload_one_item(
          env,
          totalNFTs,
          storage,
          retainAuthority,
          mutable,
          ipfsCredentials,
          awsS3Bucket,
          images[i],
          i,
          cacheContent,
          anchorProgram,
          walletKeyPair,
        ),
      );
    }
    uploadSuccessful =
      (await Promise.all(promises)).every(x => x) && uploadSuccessful;
    saveCache(cacheName, env, cacheContent);
    config = cacheContent.program.config;
    currentBatchStartIndex += BATCH_SIZE[storage];
  }

  const keys = Object.keys(cacheContent.items);
  try {
    await Promise.all(
      chunks(Array.from(Array(keys.length).keys()), 1000).map(
        async allIndexesInSlice => {
          for (
            let offset = 0;
            offset < allIndexesInSlice.length;
            offset += 10
          ) {
            const indexes = allIndexesInSlice.slice(offset, offset + 10);
            const onChain = indexes.filter(i => {
              const index = keys[i];
              return cacheContent.items[index]?.onChain || false;
            });
            const ind = keys[indexes[0]];

            if (onChain.length != indexes.length) {
              log.info(
                `Writing indices ${ind}-${keys[indexes[indexes.length - 1]]}`,
              );
              try {
                await anchorProgram.rpc.addConfigLines(
                  ind,
                  indexes.map(i => ({
                    uri: cacheContent.items[keys[i]].link,
                    name: cacheContent.items[keys[i]].name,
                  })),
                  {
                    accounts: {
                      config,
                      authority: walletKeyPair.publicKey,
                    },
                    signers: [walletKeyPair],
                  },
                );
                indexes.forEach(i => {
                  cacheContent.items[keys[i]] = {
                    ...cacheContent.items[keys[i]],
                    onChain: true,
                  };
                });
                saveCache(cacheName, env, cacheContent);
              } catch (e) {
                log.error(
                  `saving config line ${ind}-${
                    keys[indexes[indexes.length - 1]]
                  } failed`,
                  e,
                );
                uploadSuccessful = false;
              }
            }
          }
        },
      ),
    );
  } catch (e) {
    log.error(e);
  } finally {
    saveCache(cacheName, env, cacheContent);
  }
  console.log(`Done. Successful = ${uploadSuccessful}.`);
  return uploadSuccessful;
}

async function upload_one_item(
  env: string,
  totalNFTs: number,
  storage: string,
  retainAuthority: boolean,
  mutable: boolean,
  ipfsCredentials: ipfsCreds,
  awsS3Bucket: string,
  image: string,
  i: number,
  cacheContent,
  anchorProgram,
  walletKeyPair,
) {
  let uploadSuccessful = true;
  const imageName = path.basename(image);
  const index = imageName.replace(EXTENSION_PNG, '');

  if (i % 50 === 0) {
    log.info(`Processing file: ${i}`);
  } else {
    log.debug(`Processing file: ${i}`);
  }

  let link = cacheContent?.items?.[index]?.link;
  if (!link || !cacheContent.program.uuid) {
    const manifestPath = image.replace(EXTENSION_PNG, '.json');
    const manifestContent = fs
      .readFileSync(manifestPath)
      .toString()
      .replace(imageName, 'image.png')
      .replace(imageName, 'image.png');
    const manifest = JSON.parse(manifestContent);

    const manifestBuffer = Buffer.from(JSON.stringify(manifest));

    if (i === 0 && !cacheContent.program.uuid) {
      // initialize config
      log.info(`initializing config`);
      try {
        const res = await createConfig(anchorProgram, walletKeyPair, {
          maxNumberOfLines: new BN(totalNFTs),
          symbol: manifest.symbol,
          sellerFeeBasisPoints: manifest.seller_fee_basis_points,
          isMutable: mutable,
          maxSupply: new BN(0),
          retainAuthority: retainAuthority,
          creators: manifest.properties.creators.map(creator => {
            return {
              address: new PublicKey(creator.address),
              verified: true,
              share: creator.share,
            };
          }),
        });
        cacheContent.program.uuid = res.uuid;
        cacheContent.program.config = res.config.toBase58();
      } catch (exx) {
        log.error('Error deploying config to Solana network.', exx);
        throw exx;
      }
    }

    if (!link) {
      try {
        if (storage === 'arweave') {
          link = await arweaveUpload(
            walletKeyPair,
            anchorProgram,
            env,
            image,
            manifestBuffer,
            manifest,
            index,
          );
        } else if (storage === 'ipfs') {
          link = await ipfsUpload(ipfsCredentials, image, manifestBuffer);
        } else if (storage === 'aws') {
          link = await awsUpload(awsS3Bucket, image, manifestBuffer);
        }

        if (link) {
          log.debug('setting cache for ', index);
          cacheContent.items[index] = {
            link,
            name: manifest.name,
            onChain: false,
          };
          cacheContent.authority = walletKeyPair.publicKey.toBase58();
        }
      } catch (er) {
        uploadSuccessful = false;
        log.error(`Error uploading file ${index}`, er);
      }
    }
  }
  return uploadSuccessful;
}
