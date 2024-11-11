from tortoise import fields, models


class Broker(models.Model):
    broker_name = fields.CharField(max_length=100, pk=True)
    
    accounts: fields.ReverseRelation['Account']

class Account(models.Model):
    broker_name = fields.ForeignKeyField('models.Broker', to_field='broker_name', related_name='accounts')
    account_name = fields.CharField(max_length=100)
    balance = fields.DecimalField(max_digits=15, decimal_places=2, default=0.00)

    strategy_accounts: fields.ReverseRelation['StrategyAccount']

    class Meta:
        unique_together = ('broker_name', 'account_name')
        primary_key = False

class Strategy(models.Model):
    strategy_name = fields.CharField(max_length=100, pk=True)
    description = fields.TextField(null=True)
    is_active = fields.BooleanField(default=True)

    strategy_accounts: fields.ReverseRelation['StrategyAccount']

class StrategyAccount(models.Model):
    strategy_name = fields.ForeignKeyField('models.Strategy', to_field='strategy_name', related_name='strategy_accounts')
    broker_name = fields.CharField(max_length=100)
    account_name = fields.CharField(max_length=100)
    created_at = fields.DatetimeField(auto_now_add=True)
    is_active = fields.BooleanField(default=True)

    positions: fields.ReverseRelation['Position']

    class Meta:
        unique_together = ('strategy_name', 'broker_name', 'account_name')
        primary_key = False

class Position(models.Model):
    position_id = fields.IntField(pk=True)
    strategy_name = fields.ForeignKeyField('models.StrategyAccount', 'strategy_name', to_field='strategy_name')
    broker_name = fields.ForeignKeyField('models.StrategyAccount', 'broker_name', to_field='broker_name')
    account_name = fields.ForeignKeyField('models.StrategyAccount', 'account_name', to_field='account_name')
    asset = fields.CharField(max_length=50)
    position_type = fields.CharField(max_length=10)
    entry_price = fields.DecimalField(max_digits=15, decimal_places=4)
    exit_price = fields.DecimalField(max_digits=15, decimal_places=4, null=True)
    quantity = fields.FloatField()
    entry_date = fields.DatetimeField(auto_now_add=True)
    exit_date = fields.DatetimeField(null=True)
    status = fields.CharField(max_length=10, default='open')
    order_id = fields.CharField(max_length=50, null=True)
    instrument_type = fields.CharField(max_length=20, default='eq')
    exchange = fields.CharField(max_length=20)
    product = fields.CharField(max_length=10)
    stop_loss = fields.DecimalField(max_digits=15, decimal_places=4, null=True)
    take_profit = fields.DecimalField(max_digits=15, decimal_places=4, null=True)
    additional_info = fields.JSONField(null=True)

    class Meta:
        table = 'positions'

class Order(models.Model):
    order_id = fields.CharField(max_length=50, pk=True)
    strategy_name = fields.ForeignKeyField('models.StrategyAccount', 'strategy_name', to_field='strategy_name')
    broker_name = fields.ForeignKeyField('models.StrategyAccount', 'broker_name', to_field='broker_name')
    account_name = fields.ForeignKeyField('models.StrategyAccount', 'account_name', to_field='account_name')
    position = fields.ForeignKeyField('models.Position', null=True, related_name='orders')
    status = fields.CharField(max_length=20)
    trading_symbol = fields.CharField(max_length=50)
    transaction_type = fields.CharField(max_length=10)
    quantity = fields.IntField()
    filled_quantity = fields.IntField(default=0)
    price = fields.DecimalField(max_digits=15, decimal_places=4, null=True)
    order_type = fields.CharField(max_length=10)
    product = fields.CharField(max_length=10)
    exchange = fields.CharField(max_length=10)
    order_date = fields.DatetimeField(auto_now_add=True)
    validity = fields.CharField(max_length=10, null=True)
    trigger_price = fields.DecimalField(max_digits=15, decimal_places=4, null=True)
    disclosed_quantity = fields.IntField(null=True)
    tag = fields.CharField(max_length=50, null=True)
    additional_info = fields.JSONField(null=True)

    class Meta:
        table = 'orders'
