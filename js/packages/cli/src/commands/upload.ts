import fs from 'fs';
import path from 'path';

import { PublicKey } from '@solana/web3.js';
import { BN } from '@project-serum/anchor';

import log from 'loglevel';

import {
  createConfig,
  loadCandyProgram,
  loadWalletKey,
} from '../helpers/accounts';
import { loadCache, saveCache } from '../helpers/cache';
import { arweaveUpload } from '../helpers/upload/arweave';
// import { nativeArweaveUpload } from '../helpers/upload/arweave-native';
import { arweaveBundleUpload } from '../helpers/upload/arweave-bundle';
import { awsUpload } from '../helpers/upload/aws';
import { ipfsCreds, ipfsUpload } from '../helpers/upload/ipfs';
import { StorageType } from '../helpers/storage-type';
import { chunks } from '../helpers/various';

type UploadParams = {
  files: string[];
  cacheName: string;
  env: string;
  keypair: string;
  totalNFTs: number;
  storage: string;
  retainAuthority: boolean;
  mutable: boolean;
  rpcUrl: string;
  ipfsCredentials: ipfsCreds;
  awsS3Bucket: string;
  jwk: string;
};

async function initConfig(
  anchorProgram,
  walletKeyPair,
  {
    totalNFTs,
    mutable,
    symbol,
    retainAuthority,
    sellerFeeBasisPoints,
    creators,
    env,
    cache,
    cacheName,
  },
) {
  log.info('Initializing config');
  try {
    const res = await createConfig(anchorProgram, walletKeyPair, {
      maxNumberOfLines: new BN(totalNFTs),
      symbol,
      sellerFeeBasisPoints,
      isMutable: mutable,
      maxSupply: new BN(0),
      retainAuthority: retainAuthority,
      creators: creators.map(creator => ({
        address: new PublicKey(creator.address),
        verified: true,
        share: creator.share,
      })),
    });
    cache.program.uuid = res.uuid;
    cache.program.config = res.config.toBase58();
    const config = res.config;

    log.info(
      `initialized config for a candy machine with publickey: ${config.toBase58()}`,
    );

    saveCache(cacheName, env, cache);
    return config;
  } catch (err) {
    log.error('Error deploying config to Solana network.', err);
    throw err;
  }
}

function getItemManifest(dirname, item) {
  const manifestPath = path.join(dirname, `${item}.json`);
  return JSON.parse(fs.readFileSync(manifestPath).toString());
}

function getItemsNeedingUpload(items, files) {
  const all = [
    ...new Set([
      ...Object.keys(items),
      ...files.map(filePath => path.basename(filePath, path.extname(filePath))),
    ]),
  ];

  return all.filter(idx => !items[idx]?.link);
}

async function writeIndices({
  anchorProgram,
  cache,
  cacheName,
  env,
  config,
  walletKeyPair,
}) {
  const keys = Object.keys(cache.items);
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
              return cache.items[index]?.onChain || false;
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
                    uri: cache.items[keys[i]].link,
                    name: cache.items[keys[i]].name,
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
                  cache.items[keys[i]] = {
                    ...cache.items[keys[i]],
                    onChain: true,
                  };
                });
                saveCache(cacheName, env, cache);
              } catch (err) {
                log.error(
                  `saving config line ${ind}-${
                    keys[indexes[indexes.length - 1]]
                  } failed`,
                  err,
                );
              }
            }
          }
        },
      ),
    );
  } catch (e) {
    log.error(e);
  } finally {
    saveCache(cacheName, env, cache);
  }
}

function saveManifestsWithLink(cache, manifests, links, indices) {
  manifests.forEach((manifest, idx) => {
    cache.items[indices[idx]] = {
      link: links[idx],
      name: manifest.name,
      onChain: false,
    };
  }, {});
  return cache;
}

export async function upload({
  files,
  cacheName,
  env,
  keypair,
  totalNFTs,
  storage,
  retainAuthority,
  mutable,
  rpcUrl,
  ipfsCredentials,
  awsS3Bucket,
  jwk,
}: UploadParams): Promise<void> {
  const cache = loadCache(cacheName, env) || {};
  const cachedProgram = (cache.program = cache.program || {});
  const cachedItems = (cache.items = cache.items || {});

  const dirname = path.dirname(files[0]);
  const needUpload = getItemsNeedingUpload(cachedItems, files);

  let walletKeyPair;
  let anchorProgram;

  if (needUpload.length) {
    if (storage === StorageType.ArweaveNative) {
      const bundleUploads = await arweaveBundleUpload(
        dirname,
        needUpload,
        JSON.parse(fs.readFileSync(jwk).toString()),
      );

      const { updatedManifests, manifestLinks, indices } = bundleUploads.reduce(
        (acc, [bundleManifests, bundleLinks, bundleIndices]) => {
          acc.updatedManifests.push(...bundleManifests);
          acc.manifestLinks.push(...bundleLinks);
          acc.indices.push(...bundleIndices);
          return acc;
        },
        { updatedManifests: [], manifestLinks: [], indices: [] },
      );

      saveManifestsWithLink(cache, updatedManifests, manifestLinks, indices);
      saveCache(cacheName, env, cache);
    } else {
      for (const toUpload of needUpload) {
        const manifest = getItemManifest(dirname, toUpload);
        const manifestBuffer = Buffer.from(JSON.stringify(manifest));

        log.debug(`Processing file: ${toUpload}`);

        switch (storage) {
          case StorageType.Ipfs:
            await ipfsUpload(ipfsCredentials, toUpload, manifestBuffer);
            break;
          case StorageType.Aws:
            await awsUpload(awsS3Bucket, toUpload, manifestBuffer);
            break;
          case StorageType.Arweave:
          default:
            walletKeyPair = loadWalletKey(keypair);
            anchorProgram = await loadCandyProgram(walletKeyPair, env, rpcUrl);
            await arweaveUpload(
              walletKeyPair,
              anchorProgram,
              env,
              toUpload,
              manifestBuffer,
              manifest,
            );
        }
      }
    }
  }

  const {
    properties: { creators },
    seller_fee_basis_points: sellerFeeBasisPoints,
    symbol,
  } = getItemManifest(dirname, 0);

  walletKeyPair = loadWalletKey(keypair);
  anchorProgram = await loadCandyProgram(walletKeyPair, env, rpcUrl);

  const config = cachedProgram.config
    ? new PublicKey(cachedProgram.config)
    : await initConfig(anchorProgram, walletKeyPair, {
        totalNFTs,
        mutable,
        retainAuthority,
        sellerFeeBasisPoints,
        symbol,
        creators,
        env,
        cache,
        cacheName,
      });

  return writeIndices({
    anchorProgram,
    cache,
    cacheName,
    env,
    config,
    walletKeyPair,
  });
}
