from app.brokers.dhan_broker import DhanBroker

DhanBroker.initialize("ACC1")
print(DhanBroker.ltp_quote("ACC1",exchange_token="500003"))