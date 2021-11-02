import fs from 'fs';
import path from 'path';
import Arweave from 'arweave';
import { bundleAndSignData, createData, ArweaveSigner } from 'arbundles';
import { EXTENSION_PNG } from '../constants';

let _arweave;
function getArweave() {
  _arweave =
    _arweave ||
    new Arweave({
      host: 'arweave.net',
      port: 443,
      protocol: 'https',
      timeout: 20000,
      logging: false,
    });
  return _arweave;
}

const baseTags = [
  { name: 'App-Name', value: 'Metaplex Candy Machine' },
  { name: 'App-Version', value: '1.0.0' },
];

// The limit for the cumulated size of images to include in a single bundle.
// arBundles has a limit of 250MB, we use our own limit way below that to
// lower the risk for having to re-upload images if the matching manifests
// upload fail on voluminous collections.
// Change at your own risk.
const BATCH_SIZE_LIMIT = 50 * 1000 * 1000;

function getBatchRange(dirname, items) {
  let total = 0;
  let range = 0;
  for (const item of items) {
    const { size } = fs.statSync(path.join(dirname, `${item}.png`));
    total += size;
    if (total >= BATCH_SIZE_LIMIT) {
      if (range === 0) {
        throw new Error(
          `Item too big for arBundles size limit of ${BATCH_SIZE_LIMIT}.`,
        );
      }
      break;
    }
    range += 1;
  }
  return range;
}

function getImageDataItems(signer, images, dirname) {
  const tags = [...baseTags, { name: 'Content-Type', value: 'image/png' }];
  const items = images.map(image => {
    const data = fs.readFileSync(path.join(dirname, `${image}.png`));

    return {
      data,
      tags,
    };
  });

  return items.map(item => createData(item.data, signer, { tags: item.tags }));
}

function getUpdatedManifests(batch, dirname, imageLinks) {
  return batch.map((item, idx) => {
    const manifestPath = path.join(dirname, `${item}.json`);
    const manifest = JSON.parse(fs.readFileSync(manifestPath).toString());
    const imageLink = imageLinks[idx];
    manifest.image = imageLink;
    manifest.properties.files = [{ uri: imageLink, type: 'image/png' }];

    return manifest;
  });
}

function getManifestDataItems(signer, manifests) {
  const tags = [
    ...baseTags,
    { name: 'Content-Type', value: 'application/json' },
  ];
  const items = manifests.map(manifest => {
    const data = JSON.stringify(manifest);

    return { data, tags };
  });

  return items.map(item => createData(item.data, signer, { tags: item.tags }));
}

async function uploadBatchBundle(signer, arweave, jwk, dataItems) {
  const bundle = await bundleAndSignData(dataItems, signer);
  const itemTxIds = bundle.getIds();

  const tx = await bundle.toTransaction(arweave, jwk);

  await arweave.transactions.sign(tx, jwk);
  await arweave.transactions.post(tx);

  return itemTxIds.map(txId => `https://arweave.net/${txId}`);
}

export function arweaveBundleUpload(dirname, _items, jwk) {
  const signer = new ArweaveSigner(jwk);
  const arweave = getArweave();

  const items = _items.slice();

  const batches = [];
  while (items.length) {
    const range = getBatchRange(dirname, items);
    batches.push(items.splice(0, range));
  }
  console.log(batches.length);
  return Promise.all(
    batches.map(async batch => {
      const imageLinks = await uploadBatchBundle(
        signer,
        arweave,
        jwk,
        getImageDataItems(signer, batch, dirname),
      );
      const updatedManifests = getUpdatedManifests(batch, dirname, imageLinks);
      const manifestLinks = await uploadBatchBundle(
        signer,
        arweave,
        jwk,
        getManifestDataItems(signer, updatedManifests),
      );
      const indices = batch.map(image => {
        return path.basename(image).replace(EXTENSION_PNG, '');
      });
      return [updatedManifests, manifestLinks, indices];
    }),
  );
}
