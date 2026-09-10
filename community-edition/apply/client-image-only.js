// Restricted successful rollout. Existing emergency refencing remains owned by
// index.js; this module cannot apply resources, reactivate or repair drift.
const { isDeepStrictEqual } = require('node:util');
const { deploymentImageMap, imageMapSha256 } = require('../generate_script/legacy-absent-cold-rebind-profile');
const IMAGE_MAP = 'yenhubs.org/target-image-map-sha256';

const PIN = /^ghcr\.io\/yengalvez\/hubs@sha256:[a-f0-9]{64}$/;
function reject(reason = 'structure') {
  const error = new Error('client_image_only_boundary_rejected');
  error.boundaryReason = reason;
  throw error;
}
function identity(r) {
  if (!r?.apiVersion || !r?.kind || !r?.metadata?.name) reject();
  return JSON.stringify([r.apiVersion, r.kind, r.metadata.namespace || '', r.metadata.name]);
}
function normalized(r) {
  const copy = JSON.parse(JSON.stringify(r));
  delete copy.status;
  for (const field of ['resourceVersion', 'generation', 'managedFields', 'creationTimestamp']) {
    delete copy.metadata[field];
  }
  if (copy.metadata.annotations) {
    delete copy.metadata.annotations['kubectl.kubernetes.io/last-applied-configuration'];
    if (!Object.keys(copy.metadata.annotations).length) delete copy.metadata.annotations;
  }
  return copy;
}
function verifyClientImageOnly({ resources, live, proposed, namespace, fromImage, toImage }) {
  if (!PIN.test(fromImage) || !PIN.test(toImage) || fromImage === toImage || !namespace ||
      !Array.isArray(resources) || !resources.length || !Array.isArray(live) ||
      !Array.isArray(proposed)) reject('input');
  const inventory = items => {
    const map = new Map(items.map(r => [identity(r), r]));
    if (map.size !== items.length) reject('duplicate');
    return map;
  };
  const expected = inventory(resources), before = inventory(live), after = inventory(proposed);
  if (!isDeepStrictEqual([...expected.keys()].sort(), [...before.keys()].sort()) ||
      !isDeepStrictEqual([...expected.keys()].sort(), [...after.keys()].sort())) reject('inventory');
  let patch, namespacePatch = null;
  for (const [key, previous] of before) {
    const next = after.get(key);
    if (!previous.metadata.uid || !previous.metadata.resourceVersion ||
        previous.metadata.deletionTimestamp || next.metadata.deletionTimestamp ||
        next.metadata.uid !== previous.metadata.uid) reject('live_identity');
    const a = normalized(previous), b = normalized(next);
    if (previous.apiVersion === 'v1' && previous.kind === 'Namespace' &&
        previous.metadata.name === namespace && a.metadata.annotations?.[IMAGE_MAP] !== undefined) {
      const beforeMap = imageMapSha256(deploymentImageMap(live, namespace));
      const afterMap = imageMapSha256(deploymentImageMap(proposed, namespace));
      if (a.metadata.annotations[IMAGE_MAP] !== beforeMap ||
          b.metadata.annotations?.[IMAGE_MAP] !== afterMap) reject('namespace_image_binding');
      a.metadata.annotations[IMAGE_MAP] = afterMap;
      if (beforeMap !== afterMap) namespacePatch = [
        { op: 'test', path: '/metadata/uid', value: previous.metadata.uid },
        { op: 'test', path: '/metadata/resourceVersion', value: previous.metadata.resourceVersion },
        { op: 'test', path: '/metadata/annotations/yenhubs.org~1target-image-map-sha256', value: beforeMap },
        { op: 'replace', path: '/metadata/annotations/yenhubs.org~1target-image-map-sha256', value: afterMap }
      ];
    }
    if (previous.kind === 'Deployment' && previous.apiVersion === 'apps/v1' &&
        previous.metadata.namespace === namespace && previous.metadata.name === 'hubs') {
      const containers = a.spec?.template?.spec?.containers;
      if (!Array.isArray(containers) || containers.length !== 1 ||
          containers[0].name !== 'hubs' || containers[0].image !== fromImage ||
          b.spec?.template?.spec?.containers?.[0]?.image !== toImage) reject('hubs_container');
      a.spec.template.spec.containers[0].image = toImage;
      patch = [
        { op: 'test', path: '/metadata/uid', value: previous.metadata.uid },
        { op: 'test', path: '/metadata/resourceVersion', value: previous.metadata.resourceVersion },
        { op: 'test', path: '/spec/template/spec/containers/0/image', value: fromImage },
        { op: 'replace', path: '/spec/template/spec/containers/0/image', value: toImage }
      ];
    }
    if (!isDeepStrictEqual(a, b)) {
      const error = new Error('client_image_only_boundary_rejected');
      error.resourceIndex = live.indexOf(previous);
      error.sections = ['apiVersion', 'kind', 'metadata', 'spec', 'data', 'stringData', 'type', 'immutable']
        .filter(field => !isDeepStrictEqual(a[field], b[field]));
      throw error;
    }
  }
  if (!patch) reject('hubs_missing');
  return { imagePatch: patch, namespacePatch };
}
module.exports = { verifyClientImageOnly };
