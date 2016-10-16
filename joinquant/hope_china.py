import pandas as pd
from pandas import Series,DataFrame
from datetime import date, timedelta,datetime
from six import StringIO
import pickle

cacheDir = 'hopeChinaCache'

#调试选项
DEBUG_INDUSTRY = False
DEBUG_TRANSFER = False
DEBUG_STOCKPOOL = False
DEBUG_PEPB = False
    
#全局参数
RANK_METHOD = 3    #排序方法 0:pe*pb  1:利润增长率 3:按pepb/quantile比值计算 2:ROE
MAJOR_HOLD_NUM = 10  # 主要持仓数目
COMPL_HOLD_NUM =  0  # 补充持仓数目
FENJI_A_AVAILABLE = True # 分级A作为做空股指工具
FENJI_A_REQUIREMENT = 0.1 # 分级A买入条件：股票仓位低于多少时买入分级A
FENJI_A_POSITION = 0.3   # 买入分级A的仓位水平
FENJI_A_CODE     = '150051.XSHE'  # 采用的分级A代码, (上市日期：150051：2012-02-17)

PRICE_POSITION_RATIO = 0 # pe*pb每上升1%，持仓比例降PRICE_POSITION_RATIO%，默认=1
TRADE_THRESHOLD      = 0.1 # 当调仓需求大于个股额度的TRADE_THRESHOLD时，才实施交易，默认=0.1

MAX_PER_INDUSTRY = 3 # 每个行业的持仓公司上限
MAX_HOLD_RANK_LIMIT = 30 #换仓的最大允许值

STRICTER_POSITION_OPEN = True  # 更严格的开仓条件，pepb低于基准才开仓
STOCK_OUT_MAX_RATIO = 1.5  # 严进宽出，对于持仓股票，当市值是合理市值的多少倍时将其清仓
AVERAGE_QUOTA = False  # 当有股票建仓或清仓时，进行一次平均额度操作

# 算各行业历史平均PEPB
USE_UNIVERSIAL_PEXPB = False# if True 所有股票按照g.PEXPB_THR调仓，否则按照各自行业的pepb调仓，搜索此变量修改确定阈值的算法。
USE_PROFIT_POS_FILTER = False # 是否用5年利润都为正的条件筛选用于计算行业平均PEPB的股票，True的话会减少股票样本（甚至为零）
CALC_INDUSTRY_PEPB_WEEKLY = True
HISTORY_PEPB_WEEK_SPAN = 400  # 参考历史PEPB的时间段（统计时间，周）
PEPB_THRESHOLD_QUANTILE = 0.01# 将历史pepb在分位点（默认0.03）处作为pepb阈值，即pepb比参考历史时间段的1-quantile（97%）时间都低

if CALC_INDUSTRY_PEPB_WEEKLY:
    pepbFileName = '%s/industry_pepb_weekly.csv'%cacheDir
else:
    pepbFileName = '%s/industry_pepb.csv'%cacheDir

#

def initialize(context):
    # print 'initialize'
    #开启真实价格回测功能
    set_option('use_real_price', True)
    #全局参数
    #g.USE_STOCK_QUOTA = False  # 是否调整仓位 True
    # 定义符合条件的行业
    g.qualified_industry_list = []
    #考虑的年报周期
    g.consider_year_span = 5;
    #长期pe考虑的年份周期
    g.consider_year_span_pe = 8;
    #长期pe和市值的比率
    g.current_market_cap_pe_compare_radio = 0.5
    # 股票高估阈值
    g.PEXPB_THR = 22.5 * 0.5
 

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

    g.industryDict = {
        'A01':'农业',
        'A02':'林业',
        'A03':'畜牧业',
        'A04':'渔业',
        'A05':'农、林、牧、渔服务业',
        'B06':'煤炭开采和洗选业',
        'B07':'石油和天然气开采业',
        'B08':'黑色金属矿采选业',
        'B09':'有色金属矿采选业',
        'B11':'开采辅助活动',
        'C13':'农副食品加工业',
        'C14':'食品制造业',
        'C15':'酒、饮料和精制茶制造业',
        'C17':'纺织业',
        'C18':'纺织服装、服饰业',
        'C19':'皮革、毛皮、羽毛及其制品和制鞋业',
        'C20':'木材加工及木、竹、藤、棕、草制品业',
        'C21':'家具制造业',
        'C22':'造纸及纸制品业',
        'C23':'印刷和记录媒介复制业',
        'C24':'文教、工美、体育和娱乐用品制造业',
        'C25':'石油加工、炼焦及核燃料加工业',
        'C26':'化学原料及化学制品制造业',
        'C27':'医药制造业',
        'C28':'化学纤维制造业',
        'C29':'橡胶和塑料制品业',
        'C30':'非金属矿物制品业',
        'C31':'黑色金属冶炼及压延加工业',
        'C32':'有色金属冶炼和压延加工业',
        'C33':'金属制品业',
        'C34':'通用设备制造业',
        'C35':'专用设备制造业',
        'C36':'汽车制造业',
        'C37':'铁路、船舶、航空航天和其它运输设备制造业',
        'C38':'电气机械及器材制造业',
        'C39':'计算机、通信和其他电子设备制造业',
        'C40':'仪器仪表制造业',
        'C41':'其他制造业',
        'C42':'废弃资源综合利用业',
        'D44':'电力、热力生产和供应业',
        'D45':'燃气生产和供应业',
        'D46':'水的生产和供应业',
        'E47':'房屋建筑业',
        'E48':'土木工程建筑业',
        'E50':'建筑装饰和其他建筑业',
        'F51':'批发业',
        'F52':'零售业',
        'G53':'铁路运输业',
        'G54':'道路运输业',
        'G55':'水上运输业',
        'G56':'航空运输业',
        'G58':'装卸搬运和运输代理业',
        'G59':'仓储业',
        'H61':'住宿业',
        'H62':'餐饮业',
        'I63':'电信、广播电视和卫星传输服务',
        'I64':'互联网和相关服务',
        'I65':'软件和信息技术服务业',
        'J66':'货币金融服务',
        'J67':'资本市场服务',
        'J68':'保险业',
        'J69':'其他金融业',
        'K70':'房地产业',
        'L71':'租赁业',
        'L72':'商务服务业',
        'M73':'研究和试验发展',
        'M74':'专业技术服务业',
        'N77':'生态保护和环境治理业',
        'N78':'公共设施管理业',
        'P82':'教育',
        'Q83':'卫生',
        'R85':'新闻和出版业',
        'R86':'广播、电视、电影和影视录音制作业',
        'R87':'文化艺术业',
        'S90':'综合',
            }

    #设置环境
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    ## 手动设定调仓月份（如需使用手动，注销上段）
    # 年报一般在3.30号之前发布， 半年调仓一次
    #g.Transfer_date = (5,9)
   
    #if g.USE_STOCK_QUOTA:
    #    #run_daily(dapan_stoploss, time='before_open') #'before_open'是确保在其他交易操作之前判断，若清盘止损就不作其他操作了
    #    run_daily(clear_stock_quota, time='open')  # 市价单只能在开市后下单
    #else:
    #    run_daily(dapan_stoploss) #根据大盘止损,如不想加入大盘止损，注释此句即可

    # g.pool = StockPool(context)
    #g.context = context
    g.holdStocksMajor = {}   # 主要持仓的股票（前10）
    g.rankMajor = []         # 主持仓排序
    g.holdStocksCompl = {}   # 作为补充作用的副持仓股票
    g.rankCompl = []         # 副持仓排序
    g.obsvStocks = {}        # 观察股票池，满足除价格外所有其他条件的股票{stock:{'max_value': Yuan,'profit_list': [], 'industry': []}}
    g.qualStocks = []        # 满足市值条件的股票（<max_value），按PExPB（或其他标准）排序
    g.holdableStocks = []    # 持仓股票若不在qualStocks里，但还在这个集合里，可以先不清仓
    g.clearStocksDict = {}
    g.holdIndustryCountDict = {}    # 持仓中同一行业的公司数量
    if FENJI_A_AVAILABLE:
        g.fenjiA = {}        # 分级A持仓
        
    if not USE_UNIVERSIAL_PEXPB:
        try:
            g.dfIndPepb = pd.read_csv(StringIO(read_file(pepbFileName)), index_col=0, parse_dates=True)
            print('Success reading %s, industry pepb data range: %s - %s'% (pepbFileName, g.dfIndPepb.index[0].date(),g.dfIndPepb.index[-1].date()))
        except:
            g.dfIndPepb = None
            print('Warning: Fail reading or parsing %s'%pepbFileName)
        if CALC_INDUSTRY_PEPB_WEEKLY:
            run_weekly(calc_cache_industry_avg_pepb, 1, time='before_open') # 在周一进行统计（周一前的周末数据）
        else:
            run_monthly(calc_cache_industry_avg_pepb ,1, time='before_open')  # 每月(每年5月)的第一个交易日触发


    run_monthly(update_obsv_pool,1, time='before_open')  # 每月(每年5月)的第一个交易日触发
    run_weekly(balance_pool, 1, time='open') # 由于用到了get_current_data，所以在'open'时候执行了。TODO 更改到每周末收盘后执行会不会合理一点？
    run_daily(transfer, time='open')
    run_daily(update_after_trade, time='after_close')
    # run_daily(test_daily, time='after_close')
    # 按周调用，每第一个交易日开盘前评估股票价格
    #run_weekly(weekly_adjust, weekday=1, time='open')

    # 持仓股票字典{stockCode: stockQuota}
    g.stockQuotaDict = {}

def test_daily(context) :
    print 'test_daily'

def update_obsv_pool(context):
    '''
    每年年报公告后更新股票池
    '''
    # print 'update_obsv_pool'
    if len(g.obsvStocks)==0 or context.current_dt.month == 5:
        g.obsvStocks = set_up_observation_pool(context)
    else:
        return
    #g.0.7(context)
    #print('qualStocks: %d'%(len(g.qualStocks)))
    #print(g.qualStocks)

def balance_pool(context):
    '''
    调仓计算
    '''
    # print 'balance_pool'
    if len(g.obsvStocks) < 1:
        return

    evaluate_stocks(context)  # get stocks and pe pb
    if DEBUG_STOCKPOOL:
        print('Qualified Pool Size: %d; Holdable Pool Size: %d'%(len(g.qualStocks), len(g.holdableStocks)))

    #g.clearMajorDict = {}  # 要清仓的主要持仓股票
    originMajorList = list(g.holdStocksMajor.keys())
    originComplList = list(g.holdStocksCompl.keys())
    originHoldList = originMajorList + originComplList
    hasClearStock = False  # 主仓是否有股票要清仓
    hasOpenStock  = False  # 主仓是否有股票新开仓
    #for stock in originMajorList:
    for stock in originHoldList:
        #if not stock in g.qualStocks:
        if (not stock in g.holdableStocks) or (stock in g.qualStocks and g.qualStocks.index(stock)>=MAX_HOLD_RANK_LIMIT): # 超过合理价格太多的和 排名30外的清掉
            if stock in originMajorList:
                sq = g.holdStocksMajor.get(stock)
            else:
                sq = g.holdStocksCompl.get(stock)
                
            g.clearStocksDict[stock] = sq
            #hasClearStock = True
            
            for industry in sq.get_industry_list():
                if industry in g.holdIndustryCountDict:
                    g.holdIndustryCountDict[industry] -= 1
            
            if stock in originMajorList:
                del g.holdStocksMajor[stock]
                hasClearStock = True
            else:
                del g.holdStocksCompl[stock]
                
            if DEBUG_STOCKPOOL:
                if stock in originMajorList:
                    print('%s 退出主仓'%(stock))
                else:
                    print('%s 退出副仓'%(stock))


    qualStocksStack = g.qualStocks[:] # 复制一份List方便写代码
    # 补足主持仓股票
    while len(g.holdStocksMajor) < MAJOR_HOLD_NUM and len(qualStocksStack) > 0:
        stock = qualStocksStack.pop(0) # 弹出排名最前的股票
        if stock in g.holdStocksMajor: # 已经在主持仓里
            continue
        
        if stock in g.holdStocksCompl: # 在补充持仓里
            g.holdStocksMajor[stock] = g.holdStocksCompl.get(stock)  # 移到主持仓去
            g.holdStocksMajor.get(stock).set_reassign_quota_flag(True)  # 重新配额
            hasOpenStock = True
            del g.holdStocksCompl[stock]
            if DEBUG_STOCKPOOL:
                print('%s 从副仓移入主仓'%(stock))
            continue
        
        # 新进入持仓股票
        # check industy count
        exceed = False
        for industry in g.obsvStocks.get(stock).get('industry'):
            if industry in g.holdIndustryCountDict:
                if g.holdIndustryCountDict.get(industry) >= MAX_PER_INDUSTRY:
                    print('行业%s已持仓%d只股票，不能再加入此行业股票%s'%(industry,MAX_PER_INDUSTRY,stock))
                    exceed = True
                    break
        if exceed:
            continue   # 不能再增加此行业股票
        
        for industry in g.obsvStocks.get(stock).get('industry'):
            if industry in g.holdIndustryCountDict:
                g.holdIndustryCountDict[industry] += 1
            else:
                g.holdIndustryCountDict[industry]  = 1
        g.holdStocksMajor[stock] = StockQuota(stock, g.obsvStocks[stock]['industry'], 0)
        hasOpenStock = True
        if DEBUG_STOCKPOOL:
            print('%s 加入主仓'%(stock))
    # 主持仓股票排序
    g.rankMajor = list(g.holdStocksMajor.keys())
    g.rankMajor.sort(key=lambda x: g.holdableStocks.index(x))


    # 补足副持仓股票
    while len(g.holdStocksCompl) < COMPL_HOLD_NUM and len(qualStocksStack) > 0:
        stock = qualStocksStack.pop(0)
        if stock in g.holdStocksMajor or stock in g.holdStocksCompl:
            continue

        # check industy count
        exceed = False
        for industry in g.obsvStocks.get(stock).get('industry'):
            if industry in g.holdIndustryCountDict:
                if g.holdIndustryCountDict.get(industry) >= MAX_PER_INDUSTRY:
                    print('行业%s已持仓%d只股票，不能再加入此行业股票%s'%(industry,MAX_PER_INDUSTRY,stock))
                    exceed = True # 不能再增加此行业股票
                    break  
        if exceed:
            continue
        
        for industry in g.obsvStocks.get(stock).get('industry'):
            if industry in g.holdIndustryCountDict:
                g.holdIndustryCountDict[industry] += 1
            else:
                g.holdIndustryCountDict[industry]  = 1
        g.holdStocksCompl[stock] = StockQuota(stock, g.obsvStocks[stock]['industry'], 0)
        if DEBUG_STOCKPOOL:
            print('%s 加入副仓'%(stock))
            
    # 副持仓股票排序
    g.rankCompl = list(g.holdStocksCompl.keys())
    g.rankCompl.sort(key=lambda x: g.holdableStocks.index(x))

    ################# 分配额度 ##############
    # 1 总市值
    portfolioValue  = context.portfolio.portfolio_value

    if AVERAGE_QUOTA and (hasClearStock or hasOpenStock):
        quotaPerStock  = portfolioValue / MAJOR_HOLD_NUM
        print('平均各股票配额至 %d 元'%(quotaPerStock))
        for stock in g.holdStocksMajor:
            sq = g.holdStocksMajor.get(stock)
            sq.assign_quota(quotaPerStock, context)
            
    else:
        quotaLeft = portfolioValue # 求剩余配额
        fixedCount = 0 # quota已定的股票数目
        #newCount = 0
        for stock in g.holdStocksMajor:
            sq = g.holdStocksMajor.get(stock)
            #FIXME:现在是否只有新入仓的才会被设置reassign_quota_flag，原来的股票不是也要更新市值和可用额度吗？
            #FIXME:感觉上应该是每次要调仓，因为总市值变化，都需要更新原来的所有持仓股票的重新计算可分配额度，然后再按后边的pbpe来分配加仓减仓多少
            #根据回测，以上的说法不正确，没利于卖掉涨得多的股票，为了仓位，调整成买些赚得少得股票。也就是优秀生补贴差生。
            if not sq.get_reassign_quota_flag():  # 减不需要重新调整的股票的配额
                quotaLeft -= sq.evaluate_value(context)
                fixedCount += 1
            #else:
            #    newCount += 1
        # 给新加入主仓股票分派额度
        if DEBUG_STOCKPOOL:
            print('在之前已分配额度的股票数：%d，剩余额度%d元' % (fixedCount, quotaLeft))
        if fixedCount < MAJOR_HOLD_NUM:
            quotaLeftPerStock = quotaLeft/(MAJOR_HOLD_NUM-fixedCount)  # 保证每只股票初始配额不超过 1/MAJOR_HOLD_NUM
            if DEBUG_STOCKPOOL:
                print('每只新进入股票能获得 %d 元额度'%(quotaLeftPerStock))
            for stock in g.holdStocksMajor:
                sq = g.holdStocksMajor.get(stock)
                if sq.get_reassign_quota_flag():
                    sq.assign_quota(quotaLeftPerStock, context)
                    print('%s 得到配额 %d 元'%(stock, quotaLeftPerStock))

    ################# 分配现金 ##############
    # 优先级按排名
    #current_data = get_current_data()  # 回测环境函数，得到一些实时数据
    cash       = context.portfolio.cash
    cashNeeded = 0
    for stock in g.rankMajor:
        sq = g.holdStocksMajor.get(stock)
        pe = g.holdStocksPeDict.get(stock)
        pb = g.holdStocksPbDict.get(stock)
        cashNeeded += sq.make_adjust_plan(pe, pb, context)

    for stock in g.clearStocksDict:
        sq = g.clearStocksDict.get(stock)
        cashNeeded += sq.make_clear(context)

    #FIXME:副仓逻辑，考虑把副仓可以变成另外的股票或者基金，方便以后变成分级A的情况
    if cash < cashNeeded:  # 清仓副仓股票变现
        rankComplCopy = g.rankCompl[:] # 复制一份，不影响原List
        while cash<cashNeeded and len(rankComplCopy)>0:
            # sell stockCompl
            stock = rankComplCopy.pop()  # 弹出最后一个
            sq = g.holdStocksCompl.get(stock)
            cashNeeded += sq.make_clear(context)
        
        if FENJI_A_AVAILABLE and len(g.fenjiA) > 0:
            if cash < cashNeeded:
                sq = g.fenjiA.get(FENJI_A_CODE)
                g.clearStocksDict[FENJI_A_CODE] = sq
                cashNeeded += sq.make_clear(context)
                del g.fenjiA[FENJI_A_CODE]
                if DEBUG_TRANSFER:
                    print('清仓分级A，买入股票！')
            
    else: # 给副仓分点配额
        cashLeft = cash - cashNeeded
        idx = 0
        complQuota = 0.1*portfolioValue
        while cashLeft > complQuota and idx < len(g.rankCompl):
            stock   = g.rankCompl[idx]
            sq = g.holdStocksCompl.get(stock)
            pe = g.holdStocksPeDict.get(stock)
            pb = g.holdStocksPbDict.get(stock)

            sq.assign_quota(complQuota, context)
            print('%s 在副仓获得额度%d'%(stock, complQuota))
            cashNeeded += sq.make_adjust_plan(pe, pb, context)
            cashLeft = cash - cashNeeded
            idx += 1

    # 分级A
    if FENJI_A_AVAILABLE:
        cashLeft = cash - cashNeeded
        if cashLeft > portfolioValue * (1-FENJI_A_REQUIREMENT):
            g.fenjiA[FENJI_A_CODE] = StockQuota(FENJI_A_CODE, ['fenjiA'], portfolioValue*FENJI_A_POSITION)
            cashLeft -= g.fenjiA.get(FENJI_A_CODE).make_adjust_plan(0,0,context)
            if DEBUG_TRANSFER:
                print('买入分级A')

    # end of balance        
    
def transfer(context):
    '''
    实施交易, 先卖后买
    '''
    # print 'transfer'
    for stock in g.clearStocksDict:
        sq = g.clearStocksDict.get(stock)
        sq.make_order()

    for stock in g.rankMajor:
        sq = g.holdStocksMajor.get(stock)
        sq.make_order()

    for stock in g.rankCompl:
        sq = g.holdStocksCompl.get(stock)
        sq.make_order()
        
    if FENJI_A_AVAILABLE:
        for fund in g.fenjiA:
            sq = g.fenjiA.get(fund)
            sq.make_order()

def update_after_trade(context):
    '''
    每天收盘后更新信息
    '''
    # print 'update_after_trade'
    sqDict = {}
    sqDict.update(g.clearStocksDict)  # 字典的值是对象，sqDict和g.clearStocksDict的值应该是指向同一个对象
    sqDict.update(g.holdStocksMajor)
    sqDict.update(g.holdStocksCompl)
    if FENJI_A_AVAILABLE:
        sqDict.update(g.fenjiA)

    orderDict = get_orders()
    for orderID in orderDict:
        order = orderDict.get(orderID)
        
        # update available cash for each stock
        if order.status == OrderStatus.held or order.status == OrderStatus.filled:
            # 只要有成交
            sq = sqDict.get(order.security)
            if order.is_buy:
                sq.update_cash(-order.cash)
            else:
                sq.update_cash(order.cash)
            
            if order.status == OrderStatus.held:
                # 当目标交易完成
                sq.reset_trade_flag()
                
                if order.security in g.clearStocksDict: # 从清仓列表中删除
                    del g.clearStocksDict[order.security]

def evaluate_stocks(context):
    '''
    评估股票是否满足< max_value, 并按pexpb（或者其他，如利润增长率）排序    
    '''
    # print 'evaluate_stocks'
    stockList = list(g.obsvStocks.keys())
    #print(len(stockList), stockList[0])
    if len(stockList) > 0:
        dfStocksValue = get_valuation_related(stockList, context)
        qualStocks = []
        holdableStocks = []
        thres = g.PEXPB_THR
        for i in range(len(dfStocksValue)):
            code = dfStocksValue.iloc[i]['code']
            cap  = dfStocksValue.iloc[i]['market_cap'] * 100000000  # 亿->元
            pe   = dfStocksValue.iloc[i]['pe_ratio']
            pb   = dfStocksValue.iloc[i]['pb_ratio']
            
            if cap < g.obsvStocks.get(code).get('max_value') * STOCK_OUT_MAX_RATIO:
                if USE_UNIVERSIAL_PEXPB:
                    thres = g.PEXPB_THR
                else:
                    thres = calc_pepb_threshold(g.obsvStocks.get(code).get('industry'), g.dfIndPepb, context)
                holdableStocks.append([code, pe, pb, thres])
                
                if cap < g.obsvStocks.get(code).get('max_value') and pe>0 and pb>0:
                    if STRICTER_POSITION_OPEN:
                        if pe*pb <= thres:
                            qualStocks.append([code, pe, pb, thres])
                            if DEBUG_PEPB:
                                print('Qualified stock:%s, pe=%.2f, pb=%.2f, thres=%.2f'%(code,pe,pb,thres))
                    else:
                        qualStocks.append([code, pe, pb, thres])
        # 排序
        if not USE_UNIVERSIAL_PEXPB and RANK_METHOD==3: #按pepb/quantile比值计算
            qualStocks.sort(key=lambda x:x[1]*x[2]/x[3])
            holdableStocks.sort(key=lambda x:x[1]*x[2]/x[3])
        elif RANK_METHOD == 1:  # 利润增长率
            qualStocks.sort(key=lambda x:g.obsvStocks[x[0]]['profit_list'][-1]/g.obsvStocks[x[0]]['profit_list'][-g.consider_year_span], reverse=True)
            holdableStocks.sort(key=lambda x:g.obsvStocks[x[0]]['profit_list'][-1]/g.obsvStocks[x[0]]['profit_list'][-g.consider_year_span], reverse=True)
        elif RANK_METHOD==0: # 默认为pbpe排序
            qualStocks.sort(key=lambda x:x[1]*x[2])
            holdableStocks.sort(key=lambda x:x[1]*x[2])
        elif RANK_METHOD ==2: # 默认为ROE=pbpe 倒序，排序
            qualStocks.sort(key=lambda x:x[2]/x[1], reverse=True)
            holdableStocks.sort(key=lambda x:x[2]/x[1], reverse=True)
        
        #FIXME:使用一个code,pe,pb对象，可以避免存储三个holdableStocks，holdStocksPeDict，holdStocksPbDict
        # 其他地方也要跟着修改，暂时不动它了
        #g.qualStocks = qualStocks
        g.qualStocks = [x[0] for x in qualStocks]
        g.holdableStocks = [x[0] for x in holdableStocks]
        g.holdStocksPeDict = {}
        g.holdStocksPbDict = {}
        for stockInfo in holdableStocks:
            g.holdStocksPeDict[stockInfo[0]] = stockInfo[1]
            g.holdStocksPbDict[stockInfo[0]] = stockInfo[2]

    else:
        g.qualStocks = []
        g.holdStocksPeDict = {}
        g.holdStocksPbDict = {}
        g.holdableStocks = []

    if DEBUG_INDUSTRY:
        print('qualStocks: %d'%(len(g.qualStocks)))
        if len(g.qualStocks)>0:
            print(g.qualStocks)

    # end of class StockPool
    
# 每年5月全部年报公告之后建立一次股票池(观察)
def set_up_observation_pool(context):
    # print 'set_up_observation_pool'
    #if context.current_dt.month != 5:
    #    return
    # in May
    if context.current_dt.month < 5:
        evalYear = context.current_dt.year - 2
    else:
        evalYear = context.current_dt.year - 1
    #1 选行业
    qualInduList = prepare_qualified_industry_list(context, evalYear)
    #2 行业中选股票
    poolDict = {}  # {stock: {'industry:'[industry1, industry2]}, 'max_value':value}
    for industry in qualInduList:
        qualStocks = get_top_stocks_in_industry(context, industry, evalYear)
        if DEBUG_INDUSTRY:
            print('Qualified Stocks of %s in pool: %s'%(industry, str(qualStocks)))
        for stock in qualStocks:
            if stock in poolDict:
                poolDict[stock]['industry'].append(industry)
            else:
                poolDict[stock] = { 'industry': [industry],
                                    'profit_list' :  [],
                }
    #3 集中剔除5年内有亏损的股票，利用8年盈利之和获得股票的合理价格
    #5年内主营业无亏损，主营业利润大于0
    #end_year = context.current_dt.year
    end_year = evalYear + 1
    start_year = end_year - g.consider_year_span
    start_year_pf = end_year - g.consider_year_span_pe
    stockList = list(poolDict.keys())
    #sr_
    for year in range(start_year_pf, end_year):
        if year < start_year: # 仅查询net_profit, 不过滤
            q = query(
                    income.code,
                    income.net_profit
                ).filter(
                    income.code.in_(stockList),
                  )

        else:
            q = query(
                    income.code,
                    income.net_profit
                ).filter(
                    income.code.in_(stockList),  # 过滤掉亏损的企业与年报数据少于5年的企业
                    #主营业利润大于0
                    income.operating_profit > 0
                  )
        df_year_fundamental = get_fundamentals(q, statDate=str(year))
        stockListValid = list(df_year_fundamental['code'])
        netProfits = list(df_year_fundamental['net_profit'])
        for i in range(len(stockListValid)):
            stock = stockListValid[i]
            poolDict[stock]['profit_list'].append(netProfits[i])

        if year >= start_year: # filter
            stockList = stockListValid
            
    
    keys = poolDict.keys()
    for stock in keys:
        if not stock in stockList:
            del poolDict[stock]
            
    #8年总利润之和，大于市值的一半（相当于长期pe参考）,net_profit
    #NOTE 由于市值与股票价格相关，每天都在变，此处用8年利润之和推导出股票的
    #合理最大市值max_value（=sum/ratio），回测中若市值超过合理市值，则不应出现在持仓股票池中
    for stock in poolDict:
        profitSum = sum(poolDict[stock]['profit_list'])
        years     = len(poolDict[stock]['profit_list'])
        poolDict[stock]['max_value'] = profitSum / years * g.consider_year_span_pe / g.current_market_cap_pe_compare_radio

    if DEBUG_INDUSTRY:
        print('Stocks found:\n%s'%(str(poolDict)))
    print('%d stocks in observation pool'%(len(poolDict)))
    return poolDict

def prepare_qualified_industry_list(context, evalYear):
    '''
    获得当时的满足条件的行业，结果为行业代码List
    '''
    # print 'prepare_qualified_industry_list'
    #evalYear = context.current_dt.year - 1
    
    try:  # 从文件中取出预存的合格行业
        c = read_file('%s/qual_ind_%d.txt'%(cacheDir, evalYear))
        lines = c.split('\n')
        qiList = [] 
        for line in lines:
            line = line.strip()
            if len(line) > 0:
                qiList.append(line[:line.find(':')])
        g.qualified_industry_list = qiList
        print('Use cached qualified industry')
        return qiList
    except: #文件不存在
        print('No cached qualified industry, rebuild it')
        qiList = []
    
    for industry in g.index_list:
        if is_increase_each_year_industry(context,industry, evalYear):
            qiList.append(industry)
    
    
    # write to file
    c = ''
    for qi in qiList:
        c = c + '%s:%s\n'%(qi, g.industryDict.get(qi))
    write_file('%s/qual_ind_%d.txt'%(cacheDir, evalYear), c, append=False)
    
    if DEBUG_INDUSTRY:
        print('合格行业%d个（%s）: %s'%(len(qiList), evalYear, str(qiList)))
    g.qualified_industry_list = qiList
    return qiList

def is_increase_each_year_industry(context, industry, evalYear):
    '''
    判断该行业是否满足要求
    '''
    # print 'is_increase_each_year_industry'
    try:
        induStocks = get_industry_stocks(industry)
    except:
        if DEBUG_INDUSTRY:
            print('No stock in %s(%s)'%(industry, context.current_dt))
        return False

    q = query(
            income.operating_revenue  # total_operating_revenue
        ).filter(
            income.code.in_(induStocks)
          )
    #end_year = context.current_dt.year
    end_year = evalYear + 1
    start_year = end_year - g.consider_year_span
    year_fundamentals = [get_fundamentals(q, statDate=str(year)) for year in range(start_year, end_year)]

    compare_column = 'operating_revenue'
    for i in range(1, len(year_fundamentals)):
        this_year = year_fundamentals[i-1][compare_column]
        next_year = year_fundamentals[i  ][compare_column]
        if next_year.sum() < this_year.sum(): # 行业营收负增长，不符合条件
            if DEBUG_INDUSTRY:
                print('行业%s于%d年出现负增长'%(industry,start_year+i))
            return False
    return True

def get_top_stocks_in_industry(context, industry, evalYear):
    '''
    从某行业中选择较优股票
    '''
    # print 'get_top_stocks_in_industry'
    induStocks = get_industry_stocks(industry)  # NOTE 不是evalYear时间点的股票分类，是回测到的那一刻的。
    q = query(
           income.code,
           income.total_operating_revenue,
           #net_profit_to_total_revenue
           (income.net_profit / income.total_operating_revenue).label('net_profit_to_total_revenue')
        ).filter(
            income.code.in_(induStocks)
          )
    #evalYear = context.current_dt.year - 1
    df_year_fundamental = get_fundamentals(q, statDate=str(evalYear))
    
    # 选出营收前30%的股票
    N = 0.7
    dfRevenueTopN = df_year_fundamental[df_year_fundamental['total_operating_revenue'] >= df_year_fundamental['total_operating_revenue'].quantile(N)]
    stkRevenueTopN = list(dfRevenueTopN['code'])
    
    #选出利润率高于平均的股票，利润高于前50%
    dfProfitRate = df_year_fundamental[df_year_fundamental['net_profit_to_total_revenue'] >= df_year_fundamental['net_profit_to_total_revenue'].quantile(0.5)]
    stkProfetRate = list(dfProfitRate['code'])
    qualStocks = []
    for stock in stkRevenueTopN:
        if stock in stkProfetRate:
            qualStocks.append(stock)

    return qualStocks


class StockQuota:
    def __init__(self, stock, industry_list, quota=0):
        '''
        初始化持仓股票配额
        stock: stock code
        quota: 初始配额，单位是Yuan
        '''
        self.stockCode  = stock
        self.quota      = quota
        self.cash       = quota  #名义上剩余可用现金
        self.position   = 0
        self.targetValue = 0
        self.tradeFlag   = 'UNCHANGED'
        self.industryList = industry_list
        #self.cashValid  = cash_valid  #实际上可操作现金
        self.holdAmount = 0 # 持股数量
        self.holdValue  = 0 # 持股市值 TODO
        self.holdCost   = 0 # 持股成本 TODO
        self.maxCost    = 0 # 操作此股票最大占用资金数量 TODO
        
        #FIXME：没有用到？
        # 是的，改为在tradeFlag里实现了是否完成交易，所以应该删去
        #self.transferCompleted = False  # 本次是否调仓完毕
        
        if quota > 0:
            self.set_reassign_quota_flag(False)
        else:
            self.set_reassign_quota_flag(True)
    
    def get_industry_list(self):
        # print 'get_industry_list'
        return self.industryList

    def set_reassign_quota_flag(self, s):
        '''
        s: flag, True/False
        '''
        # print 'set_reassign_quota_flag'
        self.reassignQuotaFlag = s

    def get_reassign_quota_flag(self):
        # print 'get_reassign_quota_flag'
        return self.reassignQuotaFlag

    def evaluate_value(self, context):
        '''
        更新市值及额度, 将股票盈亏加到原额度上
        '''
        # print 'evaluate_value'
        position = context.portfolio.positions.get(self.stockCode)
        
        if position:
            self.holdAmount = position.total_amount
            price = position.price
            self.holdValue = price * self.holdAmount
        else:
            self.holdAmount = 0
            self.holdValue  = 0
        
        self.quota = self.holdValue + self.cash
        return self.quota
    
    def assign_quota(self, quota, context):
        '''
        分配额度
        '''
        # print 'assign_quota'
        self.evaluate_value(context)
        self.cash  += quota - self.quota
        self.quota = quota
        self.set_reassign_quota_flag(False)

    def make_adjust_plan(self, pe, pb, context):
        '''
        按价格调整仓位
        '''
        # print 'make_adjust_plan'
        if USE_UNIVERSIAL_PEXPB:
            THR = g.PEXPB_THR
        elif FENJI_A_AVAILABLE and 'fenjiA' in self.industryList:
            THR = g.PEXPB_THR
        else:
            THR = calc_pepb_threshold(self.industryList, g.dfIndPepb, context)
            
        pexpb = pe * pb

        delta = PRICE_POSITION_RATIO * (pexpb - THR) / float(THR)
        if delta < 0:
            percent = 1.0
        elif delta > 1:
            percent = 0.0
        else:
            percent = 1.0 - delta
        
        #stockPrice  = current_data[self.stockCode].day_open
        targetValue = self.quota * percent
        deltaCash   = targetValue - self.holdValue
        if DEBUG_TRANSFER:
            print('PLAN: %s from %.2f%% to %.2f%%(%d -> %d, pe=%.2f, pb=%.2f)' % (self.stockCode, self.position*100, percent*100, int(self.holdValue), int(targetValue), pe, pb))

        # 当调整幅度>10%额度时才实施交易
        if abs(deltaCash) >= TRADE_THRESHOLD * self.quota:
            self.targetValue = targetValue
            self.position    = percent
            # 需要交易的flag, 当完成目标仓位时置为UNCHANGED
            if deltaCash > 0:
                self.tradeFlag = 'BUY'
            else:
                self.tradeFlag = 'SELL'

            if DEBUG_TRANSFER:
                print('%s(%s): quota=%d, percent=%.2f, target=%d, pe=%.2f, pb=%.2f'%(self.stockCode, str(self.industryList), int(self.quota), percent*100, int(targetValue), pe, pb))
        
        
        return deltaCash
    
    def make_clear(self, context):
        '''
        清仓股票操作
        '''
        # print 'make_clear'
        self.evaluate_value(context)
        if self.holdValue > 0:
            self.targetValue = 0
            self.tradeFlag = 'SELL'
            
            if DEBUG_TRANSFER:
                print('CLEAR %s: quota=%d'%(self.stockCode, int(self.quota)))
        return -self.holdValue  # deltaCash
    
    def get_trade_flag(self):
        # print 'get_trade_flag'
        return self.tradeFlag

    def reset_trade_flag(self):
        # print 'reset_trade_flag'
        self.tradeFlag = 'UNCHANGED'

    def make_order(self):
        # print 'make_order'
        if self.tradeFlag != 'UNCHANGED':
            order_target_value(self.stockCode, self.targetValue)
    
    def update_cash(self, deltaCash):
        # print 'update_cash'
        '''
        交易完成后，更新该股票的可操作现金
        '''
        self.cash += deltaCash
        return self.cash
    
# end of class StockQuota

def get_valuation_related(stockList, context):
    # print 'get_valuation_related'
    q = query(
            valuation.code,
            valuation.market_cap,
            valuation.pe_ratio,
            valuation.pb_ratio
        ).filter(
            valuation.code.in_(stockList)
        )
        
    return get_fundamentals(q)  # date=context.current_dt

def calc_cache_industry_avg_pepb(context): # run yearly，每年5月初
    '''
    计算和缓存当年行业平均pe和pb
    '''
    # print 'calc_cache_industry_avg_pepb'
    #curDt = context.current_dt
    #curDt = datetime(curDt.year, curDt.month, curDt.day)
    curDt = context.current_dt
    if CALC_INDUSTRY_PEPB_WEEKLY==False and curDt.month != 5:
        return

    if type(g.dfIndPepb) == type(None):
        # 读csv
        try:
        #if True:
            df = pd.read_csv(StringIO(read_file(pepbFileName)), index_col=0, parse_dates=True)
            # 若数据已经存在，跳过
            #if curDt < df.index[-1].date():
            #    print('PEPB DATA is cached, PASS')
            #    return df
        except:
        #else:
            print('Fail opening nor parsing %s.'%(pepbFileName))
            df = pd.DataFrame() # 空表，用于合并
    else:
        df = g.dfIndPepb

    if len(df)>0 and curDt <= df.index[-1]:
        if DEBUG_PEPB:
            print('dfIndPepb is satisfied.')
        return df
    
    
    if curDt.month < 5:
        evalYear = curDt.year - 2  # 取年报的年限
    else:
        evalYear = curDt.year - 1 
    pepbDict = {}
    
    for industry in g.index_list:
        # 1.选出该行业当年营收，利润排名靠前的企业
        stockList = get_top_stocks_in_industry(context, industry, evalYear)
        # 2.剔除5年内有过亏损的企业
        if USE_PROFIT_POS_FILTER:
            end_year = evalYear + 1
            start_year = end_year - g.consider_year_span
            for year in range(start_year, end_year):
                if year < 2005:  # 数据库中数据从05年开始才有
                    continue
                
                q = query(
                        income.code,
                        income.net_profit
                    ).filter( # 过滤掉亏损的企业与年报数据少于5年的企业
                        income.code.in_(stockList), 
                        #主营业利润大于0
                        income.operating_profit > 0
                    )
                df_year_fundamental = get_fundamentals(q, statDate=str(year))
                stockList = list(df_year_fundamental['code'])
        if DEBUG_PEPB:
            print('Idustry %s: %d (%s)'%(industry, len(stockList), stockList))
        #3. 计算平均pe，pb
        if len(stockList) == 0:
            pepbDict['%s_pe'%industry] = -1
            pepbDict['%s_pb'%industry] = -1
            continue
        #else:
        qProfit = query(
                        income.np_parent_company_owners
                    ).filter(
                        income.code.in_(stockList)
                    )
        qAsset = query(
                        balance.total_owner_equities
                    ).filter(
                        balance.code.in_(stockList)
                    )
        qCap  = query(
                        valuation.market_cap
                    ).filter(
                        valuation.code.in_(stockList)
                    )
        dfProfit = get_fundamentals(qProfit, statDate=str(evalYear))
        dfAsset  = get_fundamentals(qAsset, statDate=str(evalYear))
        dfCap    = get_fundamentals(qCap)   # context.current_dt的前一日收盘数据
        
        avgPE = dfCap['market_cap'].sum() * (10**8) / dfProfit['np_parent_company_owners'].sum()
        avgPB = dfCap['market_cap'].sum() * (10**8) / dfAsset['total_owner_equities'].sum()
        pepbDict['%s_pe'%industry] = avgPE
        pepbDict['%s_pb'%industry] = avgPB

        if DEBUG_PEPB:
            print('PE:%.2f, PB:%.2f'%(avgPE, avgPB))
    
    dfThis = pd.DataFrame(pepbDict, index=[curDt])
    #print(pd.concat([df,dfThis]).index)
    df = pd.concat([df, dfThis])
    g.dfIndPepb = df
    #df = df.sort_index()
    if DEBUG_PEPB:
        print(df[['J66_pe', 'J66_pb']])
    # 转换成date格式后再存入文件
    dfIdxBak = df.index
    dfIdxTmp = [x.date() for x in dfIdxBak]
    df.index = dfIdxTmp
    c = df_to_csv_str(df)
    write_file(path=pepbFileName, content=c, append=False)
    df.index = dfIdxBak
    return df

def df_to_csv_str(df):
    # print 'df_to_csv_str'
    text = df.to_string(header=True, index=True)  # 注意，让df没有空值
    return ',' + '\n'.join([','.join(line.split()) for line in text.split('\n')])

def calc_pepb_threshold(induList, dfIndPepb, context):
    '''
    确定股票pepb阈值的算法
    induList:  股票所属行业或概念的列表
    dfIndPepb: 行业历史pepb的DataFrame
    '''
    # print 'calc_pepb_threshold'
    # method 1, 取pepb最低行业作为参考
    threshold = -1
    for ind in induList:
        peCol = '%s_pe'%ind
        pbCol = '%s_pb'%ind
        df = dfIndPepb[dfIndPepb.index<=context.current_dt][[peCol,pbCol]]
        # 限制参考范围
        if len(df) > HISTORY_PEPB_WEEK_SPAN:
            df = df[-HISTORY_PEPB_WEEK_SPAN:]
        # 滤掉pe，pb小于0的数据    
        df = df[(df[peCol]>0) & (df[pbCol]>0)]
        
        refPe = df[peCol].quantile(PEPB_THRESHOLD_QUANTILE)
        refPb = df[pbCol].quantile(PEPB_THRESHOLD_QUANTILE)
        pexpb = refPe * refPb
        if threshold<0 or threshold>pexpb:
            threshold = pexpb
    
    return threshold
        