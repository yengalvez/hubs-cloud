const test = require('node:test');
const assert = require('node:assert/strict');
const { verifyClientImageOnly } = require('./client-image-only');
const { deploymentImageMap, imageMapSha256 } = require('../generate_script/legacy-absent-cold-rebind-profile');
const fromImage = `ghcr.io/yengalvez/hubs@sha256:${'a'.repeat(64)}`;
const toImage = `ghcr.io/yengalvez/hubs@sha256:${'b'.repeat(64)}`;
function fixture() {
  const live = [{ apiVersion: 'apps/v1', kind: 'Deployment',
    metadata: { name: 'hubs', namespace: 'test', uid: 'hubs-uid', resourceVersion: '41' },
    spec: { replicas: 1, template: { spec: { containers: [{ name: 'hubs', image: fromImage }] } } }
  }, { apiVersion: 'v1', kind: 'Secret', metadata: {
    name: 'backend', namespace: 'test', uid: 'secret-uid', resourceVersion: '42' }, data: { key: 'fake' } }];
  const proposed = structuredClone(live);
  proposed[0].spec.template.spec.containers[0].image = toImage;
  return { resources: structuredClone(proposed), live, proposed, namespace: 'test', fromImage, toImage };
}
test('exact client digest change produces only test/test/test/replace CAS', () => {
  const { imagePatch: patch } = verifyClientImageOnly(fixture());
  assert.deepEqual(patch.map(p => p.op), ['test', 'test', 'test', 'replace']);
  assert.equal(patch[1].value, '41');
  assert.equal(patch[3].path, '/spec/template/spec/containers/0/image');
});
for (const [name, mutate] of Object.entries({
  secret: f => { f.proposed[1].data.key = 'changed'; },
  replicas: f => { f.proposed[0].spec.replicas = 0; },
  environment: f => { f.proposed[0].spec.template.spec.containers[0].env = []; },
  restart: f => { f.proposed[0].spec.template.metadata = { annotations: { 'kubectl.kubernetes.io/restartedAt': 'now' } }; },
  imageTag: f => { f.toImage = 'ghcr.io/yengalvez/hubs:latest'; },
  wrongBaseline: f => { f.fromImage = toImage; },
  uid: f => { f.proposed[0].metadata.uid = 'recreated'; },
  missingResource: f => { f.live.pop(); },
  extraResource: f => { f.proposed.push({ ...f.proposed[1], kind: 'ConfigMap' }); },
  duplicateResource: f => { f.live.push(f.live[0]); },
  deletion: f => { f.live[0].metadata.deletionTimestamp = 'now'; },
  sidecar: f => { f.proposed[0].spec.template.spec.containers.push({ name: 'extra', image: toImage }); },
  namespace: f => { f.namespace = 'wrong'; },
  noHubs: f => { f.live.shift(); f.proposed.shift(); f.resources.shift(); }
})) {
  test(`rejects ${name} without exposing resource values`, () => {
    const f = fixture(); mutate(f);
    assert.throws(() => verifyClientImageOnly(f), { message: 'client_image_only_boundary_rejected' });
  });
}
test('only server-owned metadata/status and last-applied bookkeeping are ignored', () => {
  const f = fixture();
  f.proposed[0].metadata.generation = 2;
  f.proposed[0].status = { readyReplicas: 1 };
  f.proposed[1].metadata.annotations = { 'kubectl.kubernetes.io/last-applied-configuration': 'private' };
  assert.equal(verifyClientImageOnly(f).imagePatch.length, 4);
});
test('rollback uses the same constrained comparison with digests reversed', () => {
  const f = fixture();
  [f.live, f.proposed] = [f.proposed, f.live];
  [f.fromImage, f.toImage] = [f.toImage, f.fromImage];
  assert.equal(verifyClientImageOnly(f).imagePatch[3].value, fromImage);
});
function namespaceFixture() {
  const f = fixture();
  const annotation = 'yenhubs.org/target-image-map-sha256';
  for (const list of [f.live, f.proposed]) list.push({
    apiVersion: 'v1', kind: 'Namespace', metadata: { name: 'test', uid: 'namespace', resourceVersion: '9',
      annotations: { [annotation]: imageMapSha256(deploymentImageMap(list, 'test')) } }
  });
  f.resources = structuredClone(f.proposed);
  return f;
}
test('legacy Namespace binding is derived from exact images, not caller supplied metadata', () => {
  const f = namespaceFixture();
  const result = verifyClientImageOnly(f);
  assert.equal(result.namespacePatch[3].path, '/metadata/annotations/yenhubs.org~1target-image-map-sha256');
  assert.equal(result.namespacePatch[1].value, '9');
  f.proposed[2].metadata.annotations['yenhubs.org/target-image-map-sha256'] = 'a'.repeat(64);
  assert.throws(() => verifyClientImageOnly(f), /boundary_rejected/);
});
test('Namespace binding cannot hide a changed backend image or unrelated annotation', () => {
  const f = namespaceFixture();
  f.proposed[2].metadata.annotations.domain = 'different';
  assert.throws(() => verifyClientImageOnly(f), /boundary_rejected/);
});
