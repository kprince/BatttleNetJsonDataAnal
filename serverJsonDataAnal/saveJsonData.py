# _*_ coding: utf-8 -*-
import psycopg2
import getJsonData
from datetime import datetime
import time
def timeFormat(lastModified):
    modify_date = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(lastModified/1000))
    print(modify_date)
    return modify_date
def updateSerItemCount(server,alliance,horde,neutral,lastModified):
    sql = """ update "AH_ser_itemcount" set (alliance,horde,neutral,total,modifydate)
     =(%s, %s,%s,%s,%s,%s) where 
     name_en = %s"""
    try:
        conn = psycopg2.connect(host='localhost', 
                                       database='AH', user='wowah', password='wowah')
        cursor = conn.cursor()
        data = [alliance,horde,neutral,alliance+horde+neutral,lastModified,server]
        cursor.execute(sql,data)
        cursor.close()
        conn.commit()
        conn.close() 
    except Exception as e:
        print (e)

def insertSerItemCount(server,alliance,horde,neutral,lastModified):
    sql = """ insert into "AH_ser_itemcount"(name_en,alliance,horde,neutral,total,modifydate)
    values(%s, %s,%s,%s,%s,%s)"""
    try:
        conn = psycopg2.connect(host='localhost', 
                                       database='AH', user='wowah', password='wowah')
        cursor = conn.cursor()
        data = [server,alliance,horde,neutral,alliance+horde+neutral,lastModified]
        cursor.execute(sql,data)
        cursor.close()
        conn.commit()
        conn.close() 
    except Exception as e:
        print (e)

def insertAhDB(data,params):
    sql="""insert  into "AH_ahdata"(auc, item_id,owner,bid,buyout,quantity,timeleft,rand,seed,server,initdate,camp)
            values(%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" 
   
    try:
        conn = psycopg2.connect(host='localhost', 
                                       database='AH', user='wowah', password='wowah')
        cursor = conn.cursor()
        cursor.executemany(sql,data)
        cursor.close()
        conn.commit()
        conn.close() 
    except Exception as e:
        print (e)
    
def dict2list(params,server,camp):
    value = []
    date = datetime.now()
    for i in params:
        value.append((i['auc'],i['item'],i['owner'],i['bid'],
                      i['buyout'],i['quantity'],i['timeLeft'],
                      i['rand'],i['seed'],server,date,camp))
   
    return value
def saveAhData(server,local):
    """
    Save auction hall data  to database
    """  
    rul_sta_time = datetime.now()
    print (" Getting data from BattleNet begin at: " +rul_sta_time.strftime('%Y-%m-%d %H:%M:%S'))  
    url_temp=getJsonData.getAuctionDataUrl(server,local )
    url= url_temp['url']
    timestamp = url_temp['lastModified']
    auc_data = getJsonData.getAuctionData(url, timestamp)
    server_name = auc_data['realm']['name']
    server_slug = auc_data['realm']['slug']
    print (timestamp)
    print ('server_name:'+server_name)
    print ('server_slug:'+server_slug)
    rul_end_time = datetime.now()
    print (' getting json data End At:%s Time used %s' 
       %(rul_end_time.strftime('%Y-%m-%d %H:%M:%S'), 
         rul_end_time-rul_sta_time));
    dstart_all = datetime.now()
    print ("begin at: " + dstart_all.strftime('%Y-%m-%d %H:%M:%S'))
    if len(auc_data['alliance']['auctions']) >0:
        all_data = dict2list(auc_data['alliance']['auctions'],server_name,'alliance')
        insertAhDB(all_data,'alliance')
        print ('updata ing >> alliance:%d'%(len(all_data)))
    if len(auc_data['horde']['auctions'])>0:
        hor_data = dict2list(auc_data['horde']['auctions'],server_name,'horde')
        insertAhDB(hor_data,'horde')
        print ('updata ing >> horde:%d'%(len(hor_data)))
    if len(auc_data['neutral']['auctions'])>0:
        neu_data = dict2list(auc_data['neutral']['auctions'],server_name,'neutral')
        insertAhDB(neu_data,'neu')
        print ('updata ing >> neu:%d'%(len(neu_data)))
    modify_date = timeFormat(timestamp)
    updateSerItemCount(len(all_data),len(hor_data),len(neu_data),modify_date)
    saveServer(server,server_name)
    dend_all = datetime.now()  
    print ('End At:%s Time used %s' 
       %(dstart_all.strftime('%Y-%m-%d %H:%M:%S'),
         dend_all - dstart_all));
#获取单个item数据
def saveItemData(item,area):
    '''
    Save item data to database
    '''
    try:
        conn = psycopg2.connect(host='localhost', 
                                       database='AH', user='wowah', password='wowah')
    except Exception as e:
        print (e)
    itemData=getJsonData.getItemData(item,area)
    value = []
    spell = ()#技能
    bonus = []#奖励
    spell_id= 0
    source_id = 0
    disskillrank = 0
    #奖励属性数据
    if len(itemData['bonusStats']) >0:
        
        sql1 = 'insert into "AH_bonus"(stat,amount,item_id) values(%s,%s,%s)'
        for key in itemData['bonusStats']:
            bonus.append((key['stat'],key['amount'],item))
            try:
                cursor1 = conn.cursor()
                cursor1.executemany(sql1,bonus)
                cursor1.close()
                conn.commit()
            except Exception as e:
                print (e)       
    
    if len(itemData['itemSource'])>0:
        source_id = itemData['itemSource']['sourceId']
        source = (itemData['itemSource']['sourceId'],itemData['itemSource']['sourceType'])
        sql3 = 'insert into "AH_source" (source_id,source_type) values (%s,%s)'
        try:
            cursor3 = conn.cursor()
            cursor3.execute(sql3,source)
            cursor3.close()
            conn.commit()
        except Exception as e:
            print (e)
    if itemData.__contains__('disenchantingSkillRank'):
        disskillrank= itemData['disenchantingSkillRank']
    value.append((itemData['id'],disskillrank,itemData['description'],itemData['name'],
                      itemData['icon'],itemData['stackable'],itemData['itemBind'], 
                      spell_id,
                      itemData['buyPrice'],itemData['itemClass'],itemData['itemSubClass'],itemData['containerSlots'],
                      itemData['inventoryType'],itemData['equippable'],itemData['itemLevel']
                      ,itemData['maxCount'],itemData['maxDurability'],itemData['minFactionId'],itemData['minReputation'],itemData['quality'],
                      itemData['sellPrice'],itemData['requiredSkill'],itemData['requiredLevel'],itemData['requiredSkillRank'],
                      itemData['baseArmor'],itemData['hasSockets'], itemData['isAuctionable'], itemData['armor'],
                      itemData['displayInfoId'], itemData['nameDescription'], 
                      itemData['nameDescriptionColor'], itemData['upgradable'], itemData['heroicTooltip'],datetime.now(),source_id))
    #print(value)
    sql = ''' insert into "AH_item"(item_id,dis_skill_rank,description,name,
                      icon,stackable,item_bind, spell_id,
                      buy_price,item_class,item_sub_class,container_slots,
                      inventory_type,equippable,item_level
                      ,max_count,max_durability,min_faction_id,min_reputation,quality,
                      sell_price,required_skill,required_level,required_skill_rank,
                      base_armor,has_sockets,auctionable,
                      armor,display_info_id,name_description,
                      name_des_color,upgradable,heroic_tooltip,initdate,source_id)
                      values ( %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,
                                    %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                     '''
    try:
        cursor4 = conn.cursor()
        cursor4.executemany(sql,value)
        cursor4.close()
        conn.commit()
        #conn.close() 
    except Exception as e:
        print (e)
    #宝石插槽奖励数据
    if itemData.__contains__('gemInfo'):
        sql5 = """insert into "AH_gem_bonus"(name,item_id,required_skill,required_skill_rank,min_level,item_level,type,
        min_item_level) values (%s,%s,%s,%s,%s,%s,%s,%s)"""
        gem = []
        gem.append((itemData['gemInfo']['bonus']['name'],
                    itemData['gemInfo']['bonus']['srcItemId'],
                    itemData['gemInfo']['bonus']['requiredSkillId'],
                    itemData['gemInfo']['bonus']['requiredSkillRank'],
                    itemData['gemInfo']['bonus']['minLevel'],
                    itemData['gemInfo']['bonus']['itemLevel'],
                    itemData['gemInfo']['type']['type'],
                    itemData['gemInfo']['minItemLevel']))
        try:
            cursor5 = conn.cursor()
            cursor5.executemany(sql5,gem)
            cursor5.close()
            conn.commit()
            conn.close()
        except Exception as e:
            print (e)
        #技能
####    if len( itemData['itemSpells'])>0:
        #print (itemData['itemSpells'])
       
       # spell_id=itemData['itemSpells'][0]['spell']['id']
        #spell_name=itemData['itemSpells']['spell']['name']
        #spell_des= itemData['itemSpells']['spell']['description']
        #spell_trigger=itemData['itemSpells']['trigger']
        #spell_icon =itemData ['itemSpells']['spell']['icon']
        #spell=(itemData['itemSpells'][0]['spell']['id'],itemData['itemSpells'][0]['spell']['name'],itemData['itemSpells'][0]['spell']['description'],
         #          itemData ['itemSpells'][0]['spell']['icon'],itemData['itemSpells'][0]['trigger'])
       # sql2 = 'insert into "AH_spell" (spell_id,spell_name,spell_des,spell_icon,trigger) values (%s,%s,%s,%s,%s)'
        #try:
        #    cursor2 = conn.cursor()
       #     cursor2.execute(sql2,spell)
       #     cursor2.close()
        #    conn.commit()
      #  except Exception as e:
      #      print (e)
#####
#对拍卖场数据循环获取itemJSON并保存到数据库
def saveItems():
    try:
        conn1 = psycopg2.connect(host='localhost', 
                                       database='AH', user='wowah', password='wowah')
        cursor = conn1.cursor()
        sql = """select distinct item_id from "AH_ahdata" a 
    where not exists (select 1 from "AH_item"
                     where item_id = a.item_id )
                     and item_id<> '85222'
                     and item_id <> '5108'
                     and item_id <>'23772' """
        cursor.execute(sql)
        start_time = datetime.now()
        print ('开始获取物品数据： At:%s '%(start_time.strftime('%Y-%m-%d %H:%M:%S')))
        for item in cursor.fetchall():
            try:
                saveItemData(item[0],'cn')
            except Exception as e:
                print (e)
            continue
            
        end_time = datetime.now()
        print(('获取物品数据结束： At:%s  Time used: %s'%(start_time.strftime('%Y-%m-%d %H:%M:%S'))),end_time-start_time)
        cursor.close()
        conn1.close() 
    except Exception as e:
        print (e) 
    
#saveAhData('生态船','cn')
#saveAhData('arthas','cn')                  
#saveAhData('rhonin','cn')
#saveItemData(52252,'cn')
saveItems()
#saveItemData(76687,'cn')
#a = getJsonData.getAuctionDataUrl('rhonin','cn')
#print(a)
#getJsonData.getAuctionData(a['url'],a['lastModified'])
#timeFormat(1385960909000)

def server_init():
    server = [
'风暴之眼', 
'提尔之手', 
'永夜港',  
'金色平原',
'朵丹尼尔',  
'法拉希姆',  
'阿尔萨斯',  
'阿拉索',  
'埃雷达尔',  
'艾欧纳尔',  
'安东尼达斯',  
'暗影议会',  
'奥特兰克', 
'巴尔古恩', 
'冰风岗', 
'达隆米尔',  
'耳语海岸',   
'古尔丹',  
'寒冰皇冠',  
'基尔加丹',  
'激流堡',  
'巨龙之吼', 
'卡兹格罗斯',  
'拉格纳罗斯',   
'莱斯霜语',   
'利刃之拳',  
'流沙之槌',  
'玛诺洛斯',  
'麦迪文',  
'凯尔萨斯',  
'耐奥祖',  
'克尔苏加德',  
'瑞文戴尔',  
'霜狼',  
'霜之哀伤',  
'斯坦索姆',  
'塔伦米尔',   
'提瑞斯法',  
'通灵学院',  
'希尔瓦娜斯',  
'血色十字军',   
'遗忘海岸', 
'银松森林',  
'银月',  
'鹰巢山',  
'影牙要塞', 
'阿兰卡拉',
'埃德萨拉',
'血环',  
'布莱克摩',  
'杜隆坦',  
'符文图腾',  
'鬼雾峰',   
'黑暗之矛',  
'红龙女王',   
'红云台地',  
'黄金之路',  
'回音群岛',   
'火羽山',
'迦罗娜',  
'卡格斯', 
'凯恩血蹄', 
'狂风峭壁',
'雷斧堡垒',
'雷霆号角', 
'烈风峡谷',
'烈日石居',
'雷克萨',  
'玛里苟斯',  
'纳沙塔尔',  
'诺兹多姆',   
'普罗德摩', 
'千针石林',   
'燃烧之刃',
'萨尔', 
'阿隆索斯',  
'闪光平原', 
'圣火神殿',
'双塔山',
'甜水绿洲',
'沃金', 
'熊猫酒仙',
'血牙魔王', 
'勇士岛', 
'羽月', 
'蜘蛛王国',
'自由之风',
'阿迦玛甘',
'阿克蒙德',
'埃加洛尔',
'埃苏雷格',
'艾萨拉',
'艾森娜',
'爱斯特纳',
'暗影之月',
'奥拉基尔',
'冰霜之刃',
'达斯雷玛',
'地狱咆哮',
'地狱之石',
'风暴之怒', 
'风行者',
'弗塞雷迦', 
'戈古纳斯', 
'毁灭之锤',
'火焰之树', 
'卡德加', 
'拉文凯斯',
'玛法里奥', 
'麦维影歌', 
'梅尔加尼', 
'梦境之树',
'迷雾之海', 
'耐普图隆', 
'海加尔',  
'轻风之语',  
'夏维安',  
'塞纳里奥', 
'闪电之刃', 
'石爪峰',
'泰兰德', 
'屠魔山谷',  
'伊利丹', 
'月光林地', 
'月神殿',
'战歌',
'主宰之剑',
'肯瑞托', 
'阿格拉玛',
'艾苏恩',
'安威玛尔',  
'奥达曼',
'奥蕾莉亚',   
'白银之手',
'暴风祭坛',
"藏宝港湾",
"尘风峡谷", 
"达纳斯",
"迪托马斯",
"国王之谷",
"黑龙军团",
"黑石尖塔",
"红龙军团",
"回音山",
"基尔罗格", 
"卡德罗斯",
"卡扎克",
"库德兰",
"蓝龙军团",
"雷霆之王",
"烈焰峰", 
"罗宁",
"洛萨", 
"玛多兰",
"玛瑟里顿",
"莫德古得",
"耐萨里奥",
"诺莫瑞根",
"普瑞斯托",
"燃烧平原",
"萨格拉斯",
"山丘之王",
"死亡之翼",
"索拉丁",
"索瑞森",
"铜龙军团",
"图拉扬",
"伊瑟拉"]
    for i in server:
        try:
            saveAhData(i,'cn')
        except Exception as e:
            print (e)
        continue
            
def saveServer(server_zh,server_en):
    sql = """insert into "AH_server"(name_zh,name_en) values(%s,%s)"""
    try:
        conn = psycopg2.connect(host='localhost', 
                                       database='AH', user='wowah', password='wowah')
        cursor = conn.cursor()
        data = [server_zh,server_en]
        cursor.execute(sql,data)
        cursor.close()
        conn.commit()
        conn.close() 
    except Exception as e:
        print (e)
#server_init()