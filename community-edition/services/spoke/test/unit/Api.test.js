import test from "ava";
import sinon from "sinon";
import Api from "../../src/api/Api";

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
