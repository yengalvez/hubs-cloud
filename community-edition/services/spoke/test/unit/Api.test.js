import test from "ava";
import sinon from "sinon";
import Api from "../../src/api/Api";

const LOCAL_STORE_KEY = "___hubs_store";

const tokenFor = payload => {
  const encode = value =>
    Buffer.from(JSON.stringify(value))
      .toString("base64")
      .replace(/=/g, "");
  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.`;
};

test.beforeEach(() => {
  const values = new Map();
  Object.defineProperty(global, "localStorage", {
    configurable: true,
    value: {
      getItem: key => values.get(key) || null,
      setItem: (key, value) => values.set(key, value),
      removeItem: key => values.delete(key)
    }
  });
});

test("withLoginRetry retries once after successful authentication", async t => {
  const api = new Api();
  const unauthorized = { status: 401 };
  const success = { status: 200 };
  const request = sinon.stub();
  request.onFirstCall().resolves(unauthorized);
  request.onSecondCall().resolves(success);

  const showDialog = sinon.spy((_Dialog, { onSuccess }) => onSuccess());
  const response = await api.withLoginRetry(request, showDialog);

  t.is(response, success);
  t.is(request.callCount, 2);
  t.is(showDialog.callCount, 1);
});

test("withLoginRetry does not show login or repeat a successful request", async t => {
  const api = new Api();
  const success = { status: 200 };
  const request = sinon.stub().resolves(success);
  const showDialog = sinon.spy();

  const response = await api.withLoginRetry(request, showDialog);

  t.is(response, success);
  t.is(request.callCount, 1);
  t.false(showDialog.called);
});

test("isAuthenticated accepts a non-expired token", t => {
  const token = tokenFor({ sub: "account-id", exp: Math.floor(Date.now() / 1000) + 60 });
  localStorage.setItem(LOCAL_STORE_KEY, JSON.stringify({ credentials: { token } }));

  t.true(new Api().isAuthenticated());
});

test("isAuthenticated clears an expired token", t => {
  const token = tokenFor({ sub: "account-id", exp: Math.floor(Date.now() / 1000) - 60 });
  localStorage.setItem(LOCAL_STORE_KEY, JSON.stringify({ credentials: { token } }));

  t.false(new Api().isAuthenticated());
  t.is(localStorage.getItem(LOCAL_STORE_KEY), null);
});

test("isAuthenticated clears malformed local auth state", t => {
  localStorage.setItem(LOCAL_STORE_KEY, "not-json");

  t.false(new Api().isAuthenticated());
  t.is(localStorage.getItem(LOCAL_STORE_KEY), null);
});
