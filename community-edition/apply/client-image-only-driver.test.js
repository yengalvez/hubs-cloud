const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { verifyClientImageOnly } = require('./client-image-only');
const source = fs.readFileSync(require.resolve('./index'), 'utf8');
const functions = source.slice(source.indexOf('function applyManifest()'),
  source.indexOf('function expectedDeployments()'));
const fromImage = `ghcr.io/yengalvez/hubs@sha256:${'a'.repeat(64)}`;
const toImage = `ghcr.io/yengalvez/hubs@sha256:${'b'.repeat(64)}`;
function harness({ extraChange = false, failCAS = false, stopped = false, noLease = false,
  namespaceCAS = false, failNamespaceCAS = false } = {}) {
  const previous = { apiVersion: 'apps/v1', kind: 'Deployment',
    metadata: { name: 'hubs', namespace: 'test', uid: 'uid', resourceVersion: '1' },
    spec: { replicas: 1, template: { spec: { containers: [{ name: 'hubs', image: fromImage }] } } },
    status: { readyReplicas: 1 } };
  const target = structuredClone(previous);
  target.spec.template.spec.containers[0].image = toImage;
  if (extraChange) target.spec.replicas = 2;
  const calls = [];
  const context = { clientImageOnly: true, clientImageOnlyCheck: false,
    operationLeaseGuard: noLease ? null : {}, legacyActiveCompatibility: true,
    plan: { resources: [target], activationPhase: 'legacy-active', recoveryPhase: 'legacy-active' },
    RECOVERY_CONSUMERS: ['hubs'], parentNamespace: 'test',
    process: { env: { HCCE_CLIENT_FROM_IMAGE: fromImage, HCCE_CLIENT_TO_IMAGE: toImage } },
    console: { log() {} }, JSON,
    verifyClientImageOnly(args) {
      const result = verifyClientImageOnly(args);
      // Pure module tests cover derivation. Here exercise the actual driver's
      // second-CAS success and partial-effect failure handling independently.
      if (namespaceCAS) result.namespacePatch = [
        { op: 'test', path: '/metadata/uid', value: 'namespace' },
        { op: 'test', path: '/metadata/resourceVersion', value: '9' },
        { op: 'test', path: '/metadata/annotations/yenhubs.org~1target-image-map-sha256', value: 'a'.repeat(64) },
        { op: 'replace', path: '/metadata/annotations/yenhubs.org~1target-image-map-sha256', value: 'b'.repeat(64) }
      ];
      return result;
    },
    MUTATION_TIMEOUT_MS: 30000, kubectlReadTimeoutMs: 30000,
    assertOperationLeaseHeld() { calls.push('lease'); }, recoveryLockExists: () => false,
    kubectlAbsentOnlyJson: () => ({ metadata: { uid: 'namespace' } }),
    kubectlJson(args) {
      if (args.includes('namespace')) return { metadata: { uid: 'namespace', annotations: {
        'yenhubs.org/target-image-map-sha256': 'b'.repeat(64)
      } } };
      if (args.includes('deployments')) {
        const item = structuredClone(previous); if (stopped) item.spec.replicas = 0;
        return { items: [item] };
      }
      return target;
    },
    runLeaseGuardedRead: fn => fn(),
    runLeaseGuardedMutation: (guard, fn) => { guard(); const result = fn(); guard(); return result; },
    contextArgs: args => args,
    spawnSync(_command, args) {
      if (args.includes('patch')) {
        const namespace = args.includes('namespace');
        calls.push(namespace ? 'patch-namespace' : 'patch');
        assert.equal(args.includes('--type=json'), true);
        const patch = JSON.parse(args[args.indexOf('-p') + 1]);
        assert.equal(patch[0].path, '/metadata/uid');
        assert.equal(patch[1].path, '/metadata/resourceVersion');
        return { status: (namespace ? failNamespaceCAS : failCAS) ? 1 : 0 };
      }
      if (args.includes('apply')) assert.equal(args.includes('--dry-run=server'), true);
      calls.push(args.includes('apply') ? 'dry-run' : 'read');
      return { status: 0, stdout: JSON.stringify(args.includes('apply') ? target :
        { apiVersion: 'v1', kind: 'List', items: [previous] }) };
    }
  };
  vm.createContext(context); vm.runInContext(functions, context);
  return { context, calls };
}
test('actual applyManifest function only issues guarded image CAS, never full apply', () => {
  const { context, calls } = harness(); context.applyManifest();
  assert.equal(calls.filter(c => c === 'patch').length, 1);
  assert.equal(calls.filter(c => c === 'dry-run').length, 1);
  assert.equal(calls.filter(c => c === 'lease').length, 3);
});
for (const options of [{ extraChange: true }, { stopped: true }, { noLease: true }]) {
  test(`driver refuses before mutation: ${JSON.stringify(options)}`, () => {
    const { context, calls } = harness(options);
    assert.throws(() => context.applyManifest(), /client_image_only/);
    assert.equal(calls.includes('patch'), false);
  });
}
test('CAS conflict is terminal with no unconditional fallback', () => {
  const { context, calls } = harness({ failCAS: true });
  assert.throws(() => context.applyManifest(), /client_image_only_CAS_failed/);
  assert.equal(calls.filter(c => c === 'patch').length, 1);
});
test('scope checks precede namespace creation and effect-path entry', () => {
  const main = source.slice(source.indexOf('async function main()'));
  assert.ok(main.indexOf('requireClientImageOnlyActiveRuntime();') < main.indexOf('ensureFoundationalNamespaceForLease()'));
  assert.ok(main.indexOf('if (clientImageOnlyCheck)') < main.indexOf('ensureFoundationalNamespaceForLease()'));
  assert.match(source, /if \(live === null\) \{\s+if \(clientImageOnly\) throw new Error\("client_image_only_namespace_disappeared"\)/);
});
test('derived inventory binding receives a separate guarded CAS and readback', () => {
  const { context, calls } = harness({ namespaceCAS: true });
  context.applyManifest();
  assert.equal(calls.filter(c => c === 'patch').length, 1);
  assert.equal(calls.filter(c => c === 'patch-namespace').length, 1);
  assert.equal(calls.filter(c => c === 'lease').length, 5);
});
test('second CAS failure explicitly reports partial image effect without retry or rollback guesses', () => {
  const { context, calls } = harness({ namespaceCAS: true, failNamespaceCAS: true });
  assert.throws(() => context.applyManifest(), /client_image_changed_namespace_binding_CAS_failed/);
  assert.equal(calls.filter(c => c === 'patch').length, 1);
  assert.equal(calls.filter(c => c === 'patch-namespace').length, 1);
});
