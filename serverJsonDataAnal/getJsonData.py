# _*_ coding: utf-8 -*-
'''
Created on 2013-8-24
Get auction hall data from Battlenet API
@author: Yueminger
'''
import urllib.request
import json
import sys
    
def getAuctionDataUrl(Server, Area):
    '''
    get server auction hall data url
       
    '''
    sys.getdefaultencoding()
    if Area == 'cn':
        url = 'http://www.battlenet.com.cn/api/wow/auction/data/{0}'.format(Server)
    elif Area == 'us':
        url = 'http://us.battle.net/api/wow/auction/data/{0}'.format(Server)
    elif Area == 'tw':
        url = 'http://tw.battle.net/api/wow/auction/data/{0}'.format(Server)
    elif Area == 'eu':
        url = 'http://eu.battle.net/api/wow/auction/data/{0}'.format(Server)
    print(url)
    data = urllib.request.urlopen(url)
    dataStr = data.read().decode('utf-8')
    jsonData = json.loads(dataStr)
    URL = jsonData['files'][0]
    return URL
def getAuctionData(url,timestamp):
    data = urllib.request.urlopen(url)
    dataStr = data.read().decode('utf-8')
    print (type(dataStr))
    jsonData = json.loads(dataStr)
    return jsonData

def getItemData(ItemId,Area):
    if Area == 'cn':
        url = 'http://www.battlenet.com.cn/api/wow/item/{0}'.format(ItemId)
    elif Area == 'us':
        url = 'http://us.battle.net/api/wow/item/{0}'.format(ItemId)
    elif Area == 'tw':
        url = 'http://tw.battle.net/api/wow/item/{0}'.format(ItemId)
    elif Area == 'eu':
        url = 'http://eu.battle.net/api/api/wow/item/{0}'.format(ItemId)
    print(url)
    data = urllib.request.urlopen(url)
    print (type(data))
    dataStr = data.read().decode('utf-8')
    jsonData = json.loads(dataStr)
   
    return jsonData
    
    




