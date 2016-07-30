import pandas as pd
from pandas import Series,DataFrame
from datetime import date, timedelta

def initialize(context):
    #全局参数

    g.USE_STOCK_QUOTA = True  # 是否调整仓位

    # 定义行业指数list
    # http://www.cnindex.com.cn/syl.html
    g.index_list = ['A01','A02','A03','A04','A05','B06',\
    'B07','B08','B09','B11','C13','C14','C15','C17','C18',\
    'C19','C20','C21','C22','C23','C24','C25','C26','C27',\
    'C28','C29','C30','C31','C32','C33','C34','C35','C36',\
    'C37','C38','C39','C40','C41','C42','D44','D45','D46',\
    'E47','E48','E50','F51','F52','G53','G54','G55','G56',\
    'G58','G59','H61','H62','I63','I64','I65','J66','J67',\
    'J68','J69','K70','L71','L72','M73','M74','N77','N78',\
    'P82','Q83','R85','R86','R87','S90']
    
    # 定义符合条件的行业
    g.qualified_industry_list = [
        # 'A02','A03','A04','B09','C13','C14','C18','C21','C22',
        # 'C23','C24','C26','C27','C29','C34','C36','C38','C39',
        # 'C40','C41','C42','D45','D46','E48','E50','F52','G54',
        # 'I64','I65','J66','J68','J69','K70','L71','L72','M73',
        # 'M74','N77','N78','Q83','R85','R86','R87'
        ]
    #考虑的年报周期
    g.consider_year_span = 5;
    #长期pe考虑的年份周期
    g.consider_year_span_pe = 8;
    #长期pe和市值的比率
    g.current_market_cap_pe_compare_radio = 0.5
    # 股票高估阈值
    g.PEXPB_THR = 22.5
    
    #当前行业
    g.current_industry_code = ''
    g.current_industry_stocks = []
    
    g.debug = False
    g.max_stock_num = 10
    g.max_percent_per_stock = 100 / g.max_stock_num
    
    #设置环境
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    #set_option("use_real_price", True)  # 用历史真实价格进行回测，疑问：context.portfolio里的股票数量会否自动拆分。
    
    #测试使用
    # f = 12  # 调仓频率
    # g.Transfer_date = range(1,13,12/f)

    ## 手动设定调仓月份（如需使用手动，注销上段）
    # 年报一般在3.30号之前发布， 半年调仓一次
    g.Transfer_date = (4,11)
   
    if g.USE_STOCK_QUOTA:
        #run_daily(dapan_stoploss, time='before_open') #'before_open'是确保在其他交易操作之前判断，若清盘止损就不作其他操作了
        run_daily(clear_stock_quota, time='open')  # 市价单只能在开市后下单
    else:
        run_daily(dapan_stoploss) #根据大盘止损,如不想加入大盘止损，注释此句即可

    ## 按月调用程序, 1~3经常会有假期，考虑10日做交易
    run_monthly(Transfer,10)
    
    # 按周调用，每第一个交易日开盘前评估股票价格
    if g.USE_STOCK_QUOTA:
        run_weekly(weekly_adjust, weekday=1, time='open')

    #准备行业列表 --> 放到每次Check_Stock前进行
    # prepare_qualified_industry_list()

    # 持仓股票字典{stockCode: stockQuota}
    g.stockQuotaDict = {}
    
def before_trading_start(context):   # 需确保与run_daily(dapan_stoploss, time='before_open')不冲突，结果与执行顺序无关
    if g.USE_STOCK_QUOTA:
        keys = list(g.stockQuotaDict.keys())
        for stock in keys:
            stockQuota = g.stockQuotaDict.get(stock)
            if stockQuota.clearance:
                if stockQuota.percent == 0:
                    del g.stockQuotaDict[stock]
                #else:
                #    stockQuota.adjust(context)
    if(g.debug):
        print "before_trading_start"
    
def after_trading_end(context):
    if g.USE_STOCK_QUOTA:
        update_quota_after_trading(context)
    if(g.debug):
        print "after_trading_end"
    pass


    
def update_quota_after_trading(context):
    '''
    每个交易日更新各个有交易的股票的市值和剩余现金额度
    '''
    orderDict = get_orders()
    for orderID in orderDict:
        order = orderDict.get(orderID)
        if order.filled > 0: # 有成交
            stockQuota = g.stockQuotaDict.get(order.security)
            stockQuota.update_value_after_trading(context, order)
            #stockQuota.amount = context.portfolio.positions.get(order.security).total_amount  # 更新持仓数
            #if order.is_buy:    # 更新剩余现金
            #    stockQuota.cash = stockQuota.cash - order.cash
            #else: # sell
            #    stockQuota.cash = stockQuota.cash + order.cash
    pass

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
#def handle_data(context, data):
    #if(g.debug):
    #    print "handle_data"
    # Transfer(context)
    # g.qualified_industry_list = ['C26']

# 类似网格交易，根据股票pb*pe值控制股票的仓位(run weekly)
def weekly_adjust(context):
    for stock in g.stockQuotaDict:
        stockQuota = g.stockQuotaDict.get(stock)
        stockQuota.adjust(context) # 调整仓位
    pass


# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def Transfer(context):
    months = context.current_dt.month
    if months in g.Transfer_date:
        ## 分配资金
        if len(context.portfolio.positions) < g.max_stock_num :
            Num = g.max_stock_num  - len(context.portfolio.positions)
            Cash = context.portfolio.cash/Num
        else: 
            Cash = context.portfolio.cash
        
        ## 获得Buylist
        Buylist = Check_Stocks(context)
        #Buylist = ['000422.XSHE','600309.XSHG','000338.XSHE',,'600000.XSHG','000001.XSHE']
        log.info(len(Buylist))
        # log.info(Buylist)
        if len(Buylist) > 0:
            for stock in Buylist:
                if g.USE_STOCK_QUOTA:
                    if (stock not in g.stockQuotaDict) and (len(g.stockQuotaDict)<g.max_stock_num):
                        g.stockQuotaDict[stock] = StockQuota(context, stock, Cash)
                        log.info("Adding %s" % (stock))
                else:
                    if stock not in context.portfolio.positions:
                        order_value(stock,Cash)
                        log.info("Buying %s" % (stock))
    else:
        pass

def Check_Stocks(context):
    prepare_qualified_industry_list()

    select_stocks = []
    
    qualified_industry_list = g.qualified_industry_list
    for industry_code in qualified_industry_list:
        current_industry_stocks = get_stocks_in_industry(industry_code)
        
        if len(current_industry_stocks) > 0:
            print "找到合适的股票:"
            print_list(current_industry_stocks)
            select_stocks.extend(current_industry_stocks)
    
    end_year = date.today().year
    current_year = end_year - 1        
    stock_number = g.max_stock_num
    select_stocks = get_low_pe_ratio_stocks(select_stocks, stock_number, current_year)
    return select_stocks
    
def get_stocks_in_industry(industry_code):
    if g.debug:
        print 'process industry_code:' + str(industry_code)
    g.current_industry_code = industry_code
    consider_year_span = g.consider_year_span
    consider_year_span_pe = g.consider_year_span_pe
    current_industry_stocks = get_industry_stocks(g.current_industry_code)
    #设置当前需要操作的股票
    set_universe(current_industry_stocks)
    
    
    #股票历史数据的筛选
    #5年内主营业无亏损，主营业利润大于0
    end_year = date.today().year
    start_year = date.today().year - consider_year_span
    
    for i in range(start_year, end_year):
        if g.debug:
            print str(i)+" year processing"
        industry_query = query(
            income
            ).filter(
                income.code.in_(current_industry_stocks),
                #主营业利润大于0
                income.operating_profit > 0
                )
        year_fundamental = get_year_fundamentals(industry_query, i)
        current_industry_stocks = year_fundamental.index
        
    
    if g.debug:
        print "5年内主营业无亏损，主营业利润大于0:"
        print_list(current_industry_stocks)
    
    # 8年总利润之和，大于市值的一半（相当于长期pe参考）,net_profit
    # current_industry_stocks = ['000592.XSHE','000663.XSHE'] #测试
    start_year = date.today().year - g.consider_year_span_pe
    end_year = date.today().year
    current_market_cap_pe_compare_radio = g.current_market_cap_pe_compare_radio
    
    current_stocks = []
    for stock_code in current_industry_stocks:
        current_market_cap = get_market_cap(stock_code)
        if g.debug:
            print "current_market_cap:" + str(current_market_cap)
        
        total_net_profit = get_year_sum(stock_code, 'net_profit', start_year, end_year)
        if g.debug:
            print "total_net_profit:" + str(total_net_profit)

        if total_net_profit > (current_market_cap * current_market_cap_pe_compare_radio) :
            current_stocks.append(stock_code)
    
    current_industry_stocks = current_stocks

    #当前情况筛选
    #行业，净利润率的平均值，取得当年行业的平均值
    #营业收入的前30%，数值情况，取得当年营业收入30%
    current_year = end_year - 1
    
    industry_stocks = get_industry_stocks(g.current_industry_code)
    
    industry_query = query(
            income.code,
            income.total_operating_revenue,
            #net_profit_to_total_revenue
            (income.net_profit / income.total_operating_revenue).label('net_profit_to_total_revenue')
        ).filter(
            income.code.in_(industry_stocks)
            )
    year_fundamental = get_year_fundamentals(industry_query, current_year)
    
    year_fundamental = year_fundamental[year_fundamental.net_profit_to_total_revenue > year_fundamental.net_profit_to_total_revenue.quantile(0.5)]
    year_fundamental = year_fundamental[year_fundamental.total_operating_revenue > year_fundamental.total_operating_revenue.quantile(0.3)]
    industry_stocks = year_fundamental.index
    
    current_industry_stocks = list(set(current_industry_stocks).intersection(set(industry_stocks)))
    if len(current_industry_stocks) != 0 :
        print 'qualified history fitler for industry:'+str(industry_code)
            
    
    #选取前3
    #按pe估值排序(不使用pb，主要考虑成长性)
    stock_number = 3
    current_industry_stocks = get_low_pe_ratio_stocks(current_industry_stocks,stock_number,current_year)
    return current_industry_stocks

def get_low_pe_ratio_stocks(stock_codes, stock_number, year):
    if len(stock_codes) == 0 :
        return []
        
    stock_query = query(
            valuation.code
        ).filter(
            valuation.code.in_(stock_codes)
        ).order_by(
            #按市盈率排序，市盈率小便宜
            valuation.pe_ratio
        ).limit(
            stock_number
        )
    year_fundamental = get_year_fundamentals(stock_query, year)
    return year_fundamental.index
    
def get_year_sum(stock_code, field, start_year, end_year):
    industry_query = query(
        income
        ).filter(
            income.code == stock_code
            )
    year_fundamentals = [get_year_fundamentals(industry_query, i) for i in range(start_year, end_year)]
    
    sum = 0;
    length = len(year_fundamentals)
    for i in range (0, length):
        #类似，this_year_data = this_year.sum()
        this_year_data = 0
        #预防因为上市时间不够5年，导致数组越界，
        #TODO: better way?
        if len(year_fundamentals[i][field]) > 0 :
            this_year_data = year_fundamentals[i][field][0]
        sum += this_year_data
    return sum    
    
def get_market_cap(stock_code):
    #亿元，为单位
    market_cap_unit = 100000000
    q = query(
                valuation
            ).filter(
                valuation.code == stock_code
            )
    df = get_fundamentals(q)
    current_value = 0
    # 总市值，总市值(亿元)
    if len(df['market_cap']) > 0:
        current_value = df['market_cap'][0] * market_cap_unit
    return current_value
        
# 输出符合条件的行业列表 
def prepare_qualified_industry_list():
    index_list = g.index_list

    # reset qualified_industry_list
    g.qualified_industry_list = []

    qualified_industry_list = g.qualified_industry_list
    
    if len(qualified_industry_list) != 0:
        print "already have qualified_industry_list, skipped query industry list"
        return

    for industry_code in index_list:
        get_increase_each_year_industry(industry_code)
    
    message = ''
    for industry_code in qualified_industry_list:
        message = message + "'" + str(industry_code) + "'" + ','
        
    print "qualify industry_code list: " + str(len(qualified_industry_list))
    print message
    
#总营业收入，行业最近5年，都处于上升趋势的行业（行业企业的总营收累加处于增长）
def get_increase_each_year_industry(current_industry_code):
    consider_year_span = g.consider_year_span
    g.current_industry_code = current_industry_code
    try:
        g.current_industry_stocks = get_industry_stocks(g.current_industry_code)
    except:
        log.warn('Fail getting industry stocks. Probably there is no stock in industry(code: %s) at the time the test begins with.' % (current_industry_code))
        return 
    #设置当前需要操作的股票
    set_universe(g.current_industry_stocks)

    #总营业收入，行业最近N年，都处于上升趋势
    #营业收入=主营业务收入+其他业务收入,operating_revenue
    #营业总收入=营业收入+利息收入+已赚保费+手续费及佣金收入,total_operating_revenue
    industry_query = query(
        income
        ).filter(
            income.code.in_(g.current_industry_stocks)
            )
            
    end_year = date.today().year
    start_year = date.today().year - consider_year_span
    
    #exclude end_year, because end_year financial data is not ready. 
    year_fundamentals = [get_year_fundamentals(industry_query, i) for i in range(start_year, end_year)]

    #总营业收入，行业最近N年，都处于上升趋势
    increase_each_year = True
    compare_columns = 'total_operating_revenue'
    length = len(year_fundamentals)
    for i in range (1, length):
        this_year = year_fundamentals[i-1][compare_columns]
        next_year = year_fundamentals[i][compare_columns]
        
        this_year_data = this_year.sum()
        next_year_data = next_year.sum()
        
        if(g.debug):
            print 'data compare: this:'+str(this_year_data) +', next:'+str(next_year_data)
            
        #当前年比下一年小，说明减少
        if( this_year_data > next_year_data):
            increase_each_year = False
            break;
    
    if(increase_each_year):
        if(g.debug):
            print "industry qualified:" + current_industry_code
        g.qualified_industry_list.append(current_industry_code)
        
def get_year_fundamentals(query_object, stat_year):
    quarter_fundamentals = [get_fundamentals(query_object, statDate=str(stat_year)+'q'+str(i)) for i in range(1,5)]
    concat_quarter_fundamentals = pd.concat(quarter_fundamentals)
    result = concat_quarter_fundamentals.groupby('code').sum()
    return result
     
#交易  
def dapan_stoploss(context):
    ## 根据局大盘止损，具体用法详见dp_stoploss函数说明
    #kernel=2, n=10, zs=0.1
    #kernel=2, n=20, zs=0.2
    stoploss = dp_stoploss(kernel=2, n=20, zs=0.2)
    if stoploss:
        log.info('大盘危急，清盘！')
        if g.USE_STOCK_QUOTA:
            for stock in g.stockQuotaDict:
                stockQuota = g.stockQuotaDict.get(stock)
                stockQuota.clear(context)
        elif len(context.portfolio.positions)>0:
            for stock in list(context.portfolio.positions.keys()):
                order_target(stock, 0)

        # return

def dp_stoploss(kernel=2, n=10, zs=0.03):
    '''
    方法1：当大盘N日均线(默认60日)与昨日收盘价构成“死叉”，则发出True信号
    方法2：当大盘N日内跌幅超过zs，则发出True信号
    '''
    # 止损方法1：根据大盘指数N日均线进行止损
    if kernel == 1:
        t = n+2
        hist = attribute_history('000300.XSHG', t, '1d', 'close', df=False)
        temp1 = sum(hist['close'][1:-1])/float(n)
        temp2 = sum(hist['close'][0:-2])/float(n)
        close1 = hist['close'][-1]
        close2 = hist['close'][-2]
        if (close2 > temp2) and (close1 < temp1):
            return True
        else:
            return False
    # 止损方法2：根据大盘指数跌幅进行止损
    elif kernel == 2:
        hist1 = attribute_history('000300.XSHG', n, '1d', 'close',df=False)
        if ((1-float(hist1['close'][-1]/hist1['close'][0])) >= zs):
            return True
        else:
            return False

def clear_stock_quota(context):
    for stock in g.stockQuotaDict:
        stockQuota = g.stockQuotaDict.get(stock)
        if stockQuota.clearance:
            stockQuota.adjust(context)
    pass
            
#工具类
def print_list(my_list):
    for q in my_list:
        print q
        
def describe(pd_struct):
    print pd_struct.describe()
    print pd_structi

#正数表示pe * pb, 负数表示亏损或者不合理的值
def get_last_day_pexpb(context, stock_code):
    q = query(
        valuation.pb_ratio, valuation.pe_ratio
        ).filter(
            valuation.code == stock_code
        )
    df = get_fundamentals(q, context.current_dt.date())  # 得到current_dt前一交易日的数据

    if df.empty:
        return -1

    pb = df.iloc[0]['pb_ratio']
    pe = df.iloc[0]['pe_ratio']
    return pb*pe

def get_last_day_price(context, stockCode):
    '''
    获得前一个交易日的股票收市价
    '''
    startDate = context.current_dt - timedelta(30) # 获得30日前到当日的历史价格（休市怎么也不会超过1个月吧）
    endDate   = context.current_dt
    df = get_price(stockCode, start_date=startDate, end_date=endDate, frequency = "1d", fields=['close'], fq = 'pre') # !使用了前复权价格  
    return df.iloc[-2]['close']   #[-1]是当日的价格，[-2] 是前一交易日的价格

def get_today_price(context, stockCode):
    '''
    获得当天收市价
    '''
    df = get_price(stockCode, start_date=context.current_dt, end_date=context.current_dt, frequency = "1d", fields=['close'], fq = 'pre') # !使用了前复权价格  
    return df.iloc[0]['close']   #[-1]是当日的价格，[-2] 是前一交易日的价格



class StockQuota:
    def __init__(self, context, stockCode, value, amountOwned=0): # 请在建仓时初始化
        '''
        初始化目标股票的初始仓位和现金。
        value: 指定给该股票的配额
        amountOwned：已经拥有该股票的数量，默认为0
        '''
        self.adjust_method = self._adjust_method_1 # 指定调仓方法
        self.stockCode  = stockCode
        self.value      = value         # 可用于操作此股票的原始市值（股票 + 现金）
        self.amount     = amountOwned   # 拥有此股票的数量
        self.clearance  = False         # 是否要清仓此股票，清仓后从stockQuotaDict中删除，释放配额

        price = get_last_day_price(context, stockCode)
        self.cash       = self.value - self.amount * price  # 现金
        self.percent    = self.amount * price / self.value  # 仓位
        self.percentExpected = self.percent

    
    def update_value_before_trading(self, context):
        '''
         重新计算股票市值（配额）---- 交易前
        '''
        position = context.portfolio.positions.get(self.stockCode)
        if position:
            self.amount = position.total_amount    # 若用真实历史股价回测时更新股数（应付可能的送股？回测环境是否自动拆分？）
        else:
            self.amount = 0

        #price = context.portfolio.positions.get(self.stockCode).price  # 与上一种方法应该有相同的结果
        price = get_last_day_price(context, self.stockCode)  # 交易前只能获得前一交易日收盘价
        self.value  = self.cash + self.amount * price
        pass

    def adjust(self, context):
        self.update_value_before_trading(context) # 重新计算市值
        if self.clearance:
            percent = 0.0
        else:
            percent = self.adjust_method(context)  # 决定某只股票仓位百分比
        if percent != self.percent:
            stockValueNew = self.value * percent
            order_target_value(self.stockCode, stockValueNew)
            self.percentExpected = percent
            # 检查成交后更新更新self.percent
            # update self.amount & self.cash  ---> after_trading_end()

        pass
    
    def update_value_after_trading(self, context, order):
        '''
        重新计算股票市值（配额）---- 交易后
        context:
        order: 交易单信息 Order对象
        '''
        position = context.portfolio.positions.get(self.stockCode)
        if position:
            self.amount = position.total_amount            
        else:
            self.amount = 0
        
        if order.is_buy:    # 更新剩余现金
            self.cash = self.cash - order.cash
        else: # sell
            self.cash = self.cash + order.cash
        
        price = get_today_price(context, self.stockCode)
        self.value  = self.cash + self.amount * price

        if order.status == OrderStatus.held:
            log.info("Adjusting %s SUCCEEDED (%d%% -> %d%%)." % (self.stockCode, self.percent*100, self.percentExpected*100))
            self.percent = self.percentExpected  # 调仓完成，不会触发交易
        else:
            self.percent = self.amount * price / self.value  # 调仓未完成，可能触发交易 -> 满足adjust()调仓条件


    def _adjust_method_1(self, context):
        '''
        pe*pb每超过阈值10%，减仓10%
        '''
        THR = g.PEXPB_THR
        pexpb = get_last_day_pexpb(context, self.stockCode)
        #亏损或者其他原因，如退市，先卖出
        if pexpb <= 0:
            return 0.0

        delta = float(pexpb - THR) / float(THR)
        if delta < 0:
            percent = 1.0
        elif delta > 1:
            percent = 0.0
        else:
            percent = 1.0 - int(delta*10)/10.0
        return percent
       #pass

    def clear(self, context):
        '''
        此函数可在before_open时执行，
        '''
        self.clearance = True
        #self.adjust(context)  #adjust需要在open后执行,由clear_stock_quota()触发
        pass
        
# end of class StockQuota        


