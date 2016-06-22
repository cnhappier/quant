import pandas as pd
from pandas import Series,DataFrame
from datetime import date

def initialize(context):
    #全局参数
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
    
    #当前行业
    g.current_industry_code = ''
    g.current_industry_stocks = []
    
    g.debug = False
    g.max_stock_num = 10
    g.max_percent_per_stock = 100 / g.max_stock_num
    
    #设置环境
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    
    #测试使用
    # f = 12  # 调仓频率
    # g.Transfer_date = range(1,13,12/f)

    ## 手动设定调仓月份（如需使用手动，注销上段）
    # 年报一般在3.30号之前发布， 半年调仓一次
    g.Transfer_date = (4,11)
    
    run_daily(dapan_stoploss) #根据大盘止损，如不想加入大盘止损，注释此句即可
    ## 按月调用程序, 1~3经常会有假期，考虑10日做交易
    run_monthly(Transfer,10)
    
    #准备行业列表
    prepare_qualified_industry_list()
    

def before_trading_start(context):
    if(g.debug):
        print "before_trading_start"
    
def after_trading_end(context):
    if(g.debug):
        print "end_trading_start"

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    if(g.debug):
        print "handle_data"
    # Transfer(context)
    # g.qualified_industry_list = ['C26']


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
        # Buylist = ['000422.XSHE','600309.XSHG','000338.XSHE']
        log.info(len(Buylist))
        # log.info(Buylist)
        if len(Buylist) > 0:
            for stock in Buylist:
                if stock not in context.portfolio.positions.keys():
                    order_value(stock,Cash)
                    log.info("Buying %s" % (stock))
    else:
        pass

def Check_Stocks(context):
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
    

    #一次性装载所有行业数据，判断
    year_span = max(consider_year_span, consider_year_span_pe)
    
    end_year = date.today().year
    start_year = date.today().year - year_span

    industry_query = query(
            income
            ).filter(
                income.code.in_(current_industry_stocks),
                #主营业利润大于0
                income.operating_profit > 0
                )
    year_fundamentals = [get_year_fundamentals(industry_query, i) for i in range(start_year, end_year)]

    industry_stocks_fundamentals = pd.concat(year_fundamentals)

    #股票历史数据的筛选
    #5年内主营业无亏损，主营业利润大于0
    for i in range(start_year, consider_year_span)
        query_str = 'income.code in_(current_industry_stocks) & income.operating_profit > 0 & stat_year = %s' % i
        filter_year_fundamentals = industry_stocks_fundamentals.query(query_str)
        current_industry_stocks = filter_year_fundamentals.index
    #过滤，过滤出consider_year_span 年的数据
    
    #group by code, year, 保证数量大于5，如果遇到上市年份不够的股票会错过

    #按股票，累加利润
    #获取市值比较
    
    #从开始年到结束年，逐年筛选保证
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
    g.current_industry_stocks = get_industry_stocks(g.current_industry_code)
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
    result['stat_year'] = pd.Series(stat_year , index=result.index) 
    return result
     
#交易  
def dapan_stoploss(context):
    ## 根据局大盘止损，具体用法详见dp_stoploss函数说明
    #kernel=2, n=10, zs=0.1
    #kernel=2, n=20, zs=0.2
    stoploss = dp_stoploss(kernel=2, n=20, zs=0.2)
    if stoploss:
        if len(context.portfolio.positions)>0:
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
            
#工具类
def print_list(my_list):
    for q in my_list:
        print q
        
def describe(pd_struct):
    print pd_struct.describe()
    print pd_struct
    