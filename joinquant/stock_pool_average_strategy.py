def initialize(context):
    # 定义一个全局变量, 保存要操作的股票
    g.security = ['300043.XSHE','002174.XSHE','002146.XSHE']#,,
    # 初始化此策略
    # 设置我们要操作的股票池, 这里我们只操作一支股票
    set_universe(g.security)
    g.max_price = 0

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    
    security_cash = context.portfolio.starting_cash / len(g.security)
    for s in g.security:
        security = s
        # 取得过去五天的平均价格
        average_price = data[security].mavg(5, 'close')
        average_price_10 = data[security].mavg(10, 'close')
        average_price_20 = data[security].mavg(20, 'close')
        
        # 取得上一时间点价格
        current_price = data[security].close
        # print str(average_price) + ', 20 day:' + str(average_price_20) + '; current:'+ str(current_price)
        # 取得当前的现金
        current_cash = context.portfolio.cash
        
        if current_cash < security_cash:
            security_cash = current_cash

        #如果持仓，等待低于20天均线卖出，如果空仓，等待高于20天均线买入
        # if context.portfolio.positions[security].sellable_amount > 0:
        #     if current_price < average_price_20:
        #         # 卖出股票，卖出仓位
        #         order_target(security, 0)
        #         # 记录这次卖出
        #         log.info("Selling %s" % (security))
        # elif context.portfolio.positions[security].sellable_amount == 0:
        #     if current_price > average_price_20:
        #         # 用所有 cash 买入股票
        #         order_value(security, security_cash)
        #         # 记录这次买入
        #         log.info("Buying %s" % (security))
        #一个短均线（5日），一个长均线（20日），
        #短均线上穿长均线叫“金叉”，买入信号；短均线下穿长均线叫“死叉”，卖出信号。
        #均线突破确立的点位,选取 5%
        delta = 0.05
        if context.portfolio.positions[security].sellable_amount > 0:
            if average_price < average_price_20 * (1-delta):
                # 卖出股票，卖出仓位
                order_target(security, 0)
                # 记录这次卖出
                log.info("Selling %s" % (security))
        elif context.portfolio.positions[security].sellable_amount == 0:
            if average_price > average_price_20 * (1+delta):
                # 用所有 cash 买入股票
                order_value(security, security_cash)
                # 记录这次买入
                log.info("Buying %s" % (security))
