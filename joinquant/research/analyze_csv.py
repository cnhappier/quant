# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 11:22:11 2016
runReport("transaction_a_class.csv")，传入对应的股票名。打印出默认前10的盈亏股票和交易。
列里边，_B结尾的是买入，_S结尾的表示卖出

@author: 
"""

import pandas as pd
import numpy as np

def print_log(trResult, count_limit) :
    
    # 计算盈利以及持仓日数
    trResult[u'盈利'] = trResult[u'成交额_B'] + trResult[u'成交额_S'] + trResult[u'交易佣金_B'] + trResult[u'交易佣金_S']
    trResult[u'盈利'] = trResult[u'盈利'] * -1
    trResult[u'持仓日数'] = trResult[u'日期_S'] - trResult[u'日期_B']
    trResult[u'持仓日数'] = trResult[u'持仓日数'].apply(lambda x : x.astype('timedelta64[D]') / np.timedelta64(1, 'D') )
    
    # 盈利的交易
    trResultWin = trResult[trResult[u'盈利'] > 0]

    # 亏损的交易
    trResultLoss = trResult[trResult[u'盈利'] <= 0]

    # 计算交易次数最多的股票
    trResultGrp = trResult.groupby([u'股票_B'])[u'日期_B'].count()
    # trResultGrp = trResultGrp.nlargest(10, u'日期_B')
    trResultGrp = trResultGrp.nlargest(count_limit)
    
    tradeTimes = len(trResult)
    winTimes = len(trResultWin)
    lossTimes = len(trResultLoss)
    holdMax = trResult[u'持仓日数'].max()
    holdMin = trResult[u'持仓日数'].min()
    holdCount = len(trResult[u'股票_B'].unique())
    
    winAvg = trResultWin[u'盈利'].mean()
    lossAvg = trResultLoss[u'盈利'].mean()
    
    winMax = trResultWin[u'盈利'].max()
    lossMax = trResultLoss[u'盈利'].min()
    
    winMin = trResultWin[u'盈利'].min()
    lossMin = trResultLoss[u'盈利'].max()
    
    # 凯利系数计算
    winProb = winTimes * 1.00 / tradeTimes
    wlRatio = winAvg * 1.00 / lossAvg
    kelly = (winProb - (1 - winProb) / wlRatio)

    # 盈利的股票排序
    trResultRevenueGrp = trResult.groupby([u'股票_B']).sum()
    trResultRevenueGrp = trResultRevenueGrp.loc[:,[u'盈利']]

    trResultWinGrp = trResultRevenueGrp[trResultRevenueGrp[u'盈利'] > 0]
#     trResultWinGrp = trResultWinGrp.sort_values(by=u'盈利', ascending=False).head(count_limit)
    trResultWinGrp = trResultWinGrp.sort(columns=[u'盈利'], ascending=False).head(count_limit)
    # 亏损的股票排序
    trResultLossGrp = trResultRevenueGrp[trResultRevenueGrp[u'盈利'] <= 0]
#     trResultLossGrp = trResultLossGrp.sort_values(by=u'盈利').head(count_limit)
    trResultLossGrp = trResultLossGrp.sort(columns=[u'盈利']).head(count_limit)
    
    trResultTranSort = trResult.loc[:,[u'股票_B', u'盈利', u'成交价_B', u'成交价_S', u'日期_B', u'日期_S', u'数量_B',  u'数量_S', ]]
#     trResultWinTranSort = trResultTranSort[trResultTranSort[u'盈利'] > 0].sort_values(by=u'盈利', ascending=False).head(count_limit)
    trResultWinTranSort = trResultTranSort[trResultTranSort[u'盈利'] > 0].sort(columns=[u'盈利'], ascending=False).head(count_limit)
#     trResultLossTranSort = trResultTranSort[trResultTranSort[u'盈利'] <= 0].sort_values(by=u'盈利').head(count_limit)
    trResultLossTranSort = trResultTranSort[trResultTranSort[u'盈利'] <= 0].sort(columns=[u'盈利']).head(count_limit)
    
    print (u'========================交易报告=============================')
    print u'总交易次数: {0}, 盈利次数: {1}, 亏损次数: {2}, 次数胜率：{3}'.format(tradeTimes, winTimes, lossTimes, winProb)
    print u'持仓日数最长: {0}, 持仓日数最短: {1}, 一共交易过 {2} 只股票。'.format(holdMax, holdMin, holdCount)
    print u'平均盈利: {0}, 平均亏损: {1}, 最大盈利: {2}, 最大亏损{3}'.format(winAvg, lossAvg, winMax, lossMax)
    print u'凯利系数: {0}'.format(kelly)
    print (u'========================交易报告=============================')

    print (u'---------------------最盈利股票----------------------')
    print trResultWinGrp
    print (u'-----------------------------------------------------------')

    print (u'---------------------最亏损股票----------------------')
    print trResultLossGrp  
    print (u'-----------------------------------------------------------')

    print (u'---------------------最盈利交易----------------------')        
    print trResultWinTranSort
    print (u'-----------------------------------------------------------')   

    print (u'---------------------最亏损交易----------------------')    
    print trResultLossTranSort
    print (u'-----------------------------------------------------------')    

    print (u'---------------------最频繁交易股票----------------------')   
    print trResultGrp
    print (u'-----------------------------------------------------------') 

def runReport(filename, count_limit=10) :
    # print 'runReport'
    tr = pd.read_csv(filename, encoding="gbk", parse_dates=[0]) 
    
    tr = tr.convert_objects(convert_numeric=True)
    
    # 重新计算成交额
    tr[u'成交额'] = tr[u'数量'] * tr[u'成交价']
    tr = tr.dropna()
    
    #tr = tr.sort([u'股票', u'交易类型'], ascending=[True, True])
    #tr = tr.head(10)
    df = pd.DataFrame(columns=[u'日期_B', u'时间_B', u'股票_B', u'交易类型_B', u'数量_B', u'成交价_B', u'成交额_B', u'交易佣金_B', u'日期_S', u'时间_S', u'股票_S', u'交易类型_S', u'数量_S', u'成交价_S', u'成交额_S', u'交易佣金_S'])

    dfResult = pd.DataFrame(columns=[u'日期_B'])

    #print tr, df  
    for index, row in tr.iterrows():
        #row[u'日期'] = pd.to_datetime(row[u'日期'])
        rowRet = None
        if row[u'交易类型'] == u'买' :
            rowRet = row.rename_axis(lambda x : x + "_B")
            
            # 查询是否仍然有持仓
            qry = ((df[u'股票_B'] == row[u'股票']) & (df[u'股票_S'].isnull()))
            dquery = df[qry]
            if len(dquery) < 1 :
                # 没有持仓的情况
                df = df.append(rowRet)
            else :
                # 有持仓的情况
                dquery = dquery.append(rowRet)
                rowRet[u'数量_B'] = np.sum(dquery[u'数量_B'])
                rowRet[u'成交额_B'] = np.sum(dquery[u'成交额_B'])
                rowRet[u'成交价_B'] = rowRet[u'成交额_B'] / rowRet[u'数量_B']
                rowRet[u'交易佣金_B'] = np.sum(dquery[u'交易佣金_B'])
                df = df[~qry]
                df = df.append(rowRet)
        elif row[u'交易类型'] == u'卖' :


            rowRetS = row.rename_axis(lambda x : x + "_S")
            rowRetB = row.rename_axis(lambda x : x + "_B")
            
            # 查询是否仍然有持仓
            qry = ((df[u'股票_B'] == row[u'股票']) & (df[u'股票_S'].isnull()))
            dquery = df[qry]
            if len(dquery) < 1 :
                # 没有持仓的情况
                pass
            else :
                # 有持仓的情况
                rowRetB[u'日期_B'] = max(dquery[u'日期_B'])
                rowRetB[u'时间_B'] = max(dquery[u'时间_B'])
                rowRetB[u'股票_B'] = max(dquery[u'股票_B'])
                rowRetB[u'交易类型_B'] = max(dquery[u'交易类型_B'])
                rowRetB[u'数量_B'] = np.sum(dquery[u'数量_B']) + rowRetS[u'数量_S']
                rowRetB[u'成交价_B'] = np.max(dquery[u'成交价_B'])
                rowRetB[u'成交额_B'] = np.sum(dquery[u'成交额_B']) + rowRetS[u'成交额_S']
                rowRetB[u'交易佣金_B'] = np.sum(dquery[u'交易佣金_B']) * (1 + rowRetS[u'数量_S'] / np.sum(dquery[u'数量_B']))
                
                rowRetS[u'日期_B'] = dquery[u'日期_B'].values
                rowRetS[u'日期_B'] = max(dquery[u'日期_B'])
                rowRetS[u'时间_B'] = max(dquery[u'时间_B'])
                rowRetS[u'股票_B'] = max(dquery[u'股票_B'])
                rowRetS[u'交易类型_B'] = max(dquery[u'交易类型_B'])
                rowRetS[u'数量_B'] = rowRetS[u'数量_S'] * -1
                
                if rowRetB[u'成交额_B'] > 0 :
                    rowRetS[u'成交额_B'] = rowRetS[u'数量_S'] * -1 * rowRetB[u'成交价_B']
                else :
                    rowRetS[u'成交额_B'] = np.sum(dquery[u'成交额_B'])
                    
                rowRetS[u'成交价_B'] = rowRetB[u'成交价_B']
                rowRetS[u'交易佣金_B'] = np.sum(dquery[u'交易佣金_B']) * (-1 * rowRetS[u'数量_S'] / np.sum(dquery[u'数量_B']))
                
                df = df[~qry]
                df = df.append(rowRetS)
                if rowRetB[u'数量_B'] > 0 and rowRetB[u'成交额_B'] > 0 :
                    df = df.append(rowRetB)
    
    #print tr, df    
    print_log(df, count_limit)
    # print df