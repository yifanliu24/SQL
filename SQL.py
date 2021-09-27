*******************************************中酒协的数据需求代码*********************************************************
------说明-----
#1.按照建表-需求的对应顺序，即每一个中间表后面都跟着需要用到这个中间表的所有需求，
#2.在每一个需求前面都有No.1 2 3顺序，分别对应的是Excel表（表名：问题reply）所对应需求的序列号，
#3.之后再提取下个月的数据时，需要重新建表，修改相应的建表日期，
#4.需求No.11和No.21-25是同一个需求，合并在一起，
#5.需求No.15和No.26-30两个需求合并，都提供了百分比（人次和父订单是同一个维度的需求，因为都需要count(distinct parent_sale_ord_id)这个逻辑来计算人次和父订单），
#6.需求No.34和No.35这两个需求，合并在一起提取了，

####################################################配置############################################################
set mapreduce.job.running.reduce.limit=400;
set mapreduce.job.running.map.limit=500;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.job.reduce.slowstart.completedmaps =1.0;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec ;
set io.compression.codecs=com.hadoop.compression.lzo.LzopCodec ;
set hive.merge.smallfiles.avgsize=134217728;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.auto.convert.join = true ;
set hive.archive.enabled= true;
set hive.archive.har.parentdir.settable= true;
set har.partfile.size=1099511627776;


###################################建wine_order_one_year的订单表########################################################
create table ads_inno.yifan_wine_order_one_year_2 as
select 
sale_ord_id,--'销售订单编号',
sale_qtty,--'销售数量',
user_log_acct,--'用户登陆账号',
item_sku_id,--'商品SKU编号',
item_sku_name,--'SKU商品名称',
brand_cd,--'品牌名称',
ord_flag,
case
			when item_second_cate_cd = '12260'
			then '白酒'
			when item_second_cate_cd = '14714'
			then '葡萄酒'
			when item_second_cate_cd = '14715'
			then '洋酒'
			when item_second_cate_cd = '14716'
			then '啤酒'
			when item_second_cate_cd = '14717'
			then '黄酒/养生酒'
		end as item_second_cate_name, --'二级类目名称',
item_first_cate_cd,--'商品一级分类代码',
item_second_cate_cd,--'商品二级分类代码',
item_third_cate_cd,--'商品三级分类代码',
type,--'经营模式 自营：B2C 第三方：POP',
parent_sale_ord_id,--'父销售订单编号',
after_prefr_amount --'优惠后金额'
from adm.adm_m04_ord_det_sum_snapshot
where 
dt >= '2018-03-01'
and dt <= '2019-02-28'
and sale_ord_dt = dt 
and sale_ord_valid_flag='1'---订单有效标识
and split_status_cd <> 1--- 1= 拆完单后父单（卡出来不需要拆分的父单和拆后的子单）
and after_sale_flag <> 1 -- 不要换货的单，换货是换新，不是买了东西不喜欢然后取消订单换货（售后订单标识，售后的情况不要，因为属于换新）
and b2c_distribute_flag <> 1 -- 自营（卡分销情况，给一些小平台的订单，不算做JD给消费者的情况）
and xintonglu_flag <> 1  -- 线下店铺  （新通路）  
and item_third_cate_cd in (
'9435',
'9436',
'9437',
'9438',
'9439',
'12261',
'14743',
'14744',
'14746',
'14756',
'14757',
'15601',
'15602',
'15604',
'15605',
'15606')
###查数据量
select count(1) from ads_inno.yifan_wine_order_one_year_2 ---47517194


#No.16
#说明：因为白酒价格分区整体偏高，而其他四种酒类的价格分区比较接近，所以在划分的时候，把白酒划为一档，其他四种酒类（葡萄酒，洋酒，啤酒，黄酒/养生酒）划为一档
###消费金额分布
#白酒
#提数逻辑：单位：人次，1.计算在二级类目下，每个父订单去重后的金额总数，2.计算每单（父订单）在价格分区下的的数据量和百分比
select
item_second_cate_name as '酒类',
concat(round(((count( case when money < '200' then 1 else null end) / count(1))*100),2),'%') as '199及以下占比',
concat(round(((count( case when money >= '200' and money < '400' then 1 else null end) / count(1))*100),2),'%') as '200-399占比',
concat(round(((count( case when money >= '400' and money < '1500' then 1 else null end) / count(1))*100),2),'%') as '400-1499占比',
concat(round(((count( case when money >= '1500' and money < '4700' then 1 else null end) / count(1))*100),2),'%')as '1500-4699占比',
concat(round(((count( case when money >= '4700' then 1 else null end) / count(1))*100),2),'%') as '4700及以上占比', 
count( case when money < '200' then 1 else null end) as '199及以下',
count( case when money >= '200' and money < '400' then 1 else null end) as '200-399',
count( case when money >= '400' and money < '1500' then 1 else null end) as '400-1499',
count( case when money >= '1500' and money < '4700' then 1 else null end) as '1500-4699',
count( case when money >= '4700' then 1 else null end) as '4700及以上',
count(1)as '总计'
from
	(
		select
			parent_sale_ord_id,
			item_second_cate_name,
			sum(distinct after_prefr_amount) as money  --每单去重后的总金额
		from
			ads_inno.yifan_wine_order_one_year_2
		where
			item_second_cate_cd = '12260' --白酒
            and substring(ord_flag, 40, 1) not in ('1','2','3','4','5','6','7') --剔除大宗订单
		group by
		   parent_sale_ord_id,
			item_second_cate_name,
        after_prefr_amount
		order by
			money desc --倒序排序
	)
	a   
group by
	item_second_cate_name


#No.17-20
##葡萄酒，啤酒，洋酒，黄酒/养生酒
#提数逻辑：具体参考白酒
select
item_second_cate_name as '酒类',
concat(round(((count( case when money < '50' then 1 else null end) / count(1))*100),2),'%') as '49及以下占比',
concat(round(((count( case when money >= '50' and money < '100' then 1 else null end) / count(1))*100),2),'%') as '50-99占比',
concat(round(((count( case when money >= '100' and money < '300' then 1 else null end) / count(1))*100),2),'%') as '100-299占比',
concat(round(((count( case when money >= '300' and money < '1000' then 1 else null end) / count(1))*100),2),'%')as '300-999占比',
concat(round(((count( case when money >= '1000' then 1 else null end) / count(1))*100),2),'%') as '1000及以上占比', 
count( case when money < '50' then 1 else null end) as '49及以下',
count( case when money >= '50' and money < '100' then 1 else null end) as '50-99',
count( case when money >= '100' and money < '300' then 1 else null end) as '100-399',
count( case when money >= '300' and money < '1000' then 1 else null end) as '300-999',
count( case when money >= '1000' then 1 else null end) as '1000及以上',
count(1)as '总计'
from
	(
		select
			parent_sale_ord_id,
			item_second_cate_name,
			sum(distinct after_prefr_amount) as money 
		from
			ads_inno.yifan_wine_order_one_year_2
		where
			item_second_cate_cd in ( '14714', '14715', '14716', '14717')
            and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
		group by
		   parent_sale_ord_id,
			item_second_cate_name,
        after_prefr_amount
		order by
			money desc
	)
	a   
group by
	item_second_cate_name


##############################################建wine_order_three_month的订单表############################################
create table ads_inno.yifan_wine_order_three_month as
select 
sale_ord_id,--'销售订单编号',
sale_qtty,--'销售数量',
user_log_acct,--'用户登陆账号',
item_sku_id,--'商品SKU编号',
item_sku_name,--'SKU商品名称',
brand_cd,--'品牌名称',
item_first_cate_cd,--'商品一级分类代码',
item_second_cate_cd,--'商品二级分类代码',
item_third_cate_cd,--'商品三级分类代码',
type,--'经营模式 自营：B2C 第三方：POP',
parent_sale_ord_id,--'父销售订单编号',
after_prefr_amount,--'优惠后金额',
sale_ord_tm,--'下单时间',
free_goods_flag,--'赠品标识,1为赠品',
delv_way_cd,--'配送方式代码',
ord_flag,--'订单标识',
case 
  when item_second_cate_cd='12260' then '白酒'
  when item_second_cate_cd='14714' then '葡萄酒'
  when item_second_cate_cd='14716' then '啤酒'
  when item_second_cate_cd='14715' then '洋酒'
  when item_second_cate_cd='14717' then '黄酒/养生酒'
end as item_second_cate_name --'二级类目名称'
from adm.adm_m04_ord_det_sum_snapshot
where 
dt >= '2018-12-01'
and dt <= '2019-02-28'
and sale_ord_dt = dt 
and sale_ord_valid_flag='1'---订单有效标识
and split_status_cd <> 1--- 1= 拆完单后父单（卡出来不需要拆分的父单和拆后的子单）
and after_sale_flag <> 1 -- 不要换货的单，换货是换新，不是买了东西不喜欢然后取消订单换货（售后订单标识，售后的情况不要，因为属于换新）
and b2c_distribute_flag <> 1 -- 自营（卡分销情况，给一些小平台的订单，不算做JD给消费者的情况）
and xintonglu_flag <> 1  -- 线下店铺  （新通路）  
---and is_deal_ord ='1' ----(1判断是成交订单，已包含sale_ord_valid_flag订单有效标识)
and item_third_cate_cd in (
'9435',
'9436',
'9437',
'9438',
'9439',
'12261',
'14743',
'14744',
'14746',
'14756',
'14757',
'15601',
'15602',
'15604',
'15605',
'15606')
###（group by sale_ord_id,sale_qtty,user_log_acct,item_sku_id,item_sku_name,brand_cd,item_first_cate_cd,
#item_second_cate_cd,item_third_cate_cd,type,parent_sale_ord_id,after_prefr_amount,
#sale_ord_tm,free_goods_flag,delv_way_cd,ord_flag）
**--说明把group by注释掉是因为group by有聚合性和去重的作用，加上group by建表的时候，数据会相应的减少--**
###查数据量
select count(1) from ads_inno.yifan_wine_order_three_month  ---18168064


#No.38
###最近三个月订单数分布（父订单维度）
#提数逻辑：单位：父订单，1.计算每个user_log_acct的购买次数，因在父订单（count(parent_sale_ord_id)）的维度下，需要看每一单的情况，所以不需要去重，
#2.计算每种购买次数的数据量和百分比
select  
concat(round(((count(case when frequency='1' then 1 else null end)/count(1))*100),2),'%') as '购买1次占比',
concat(round(((count(case when frequency>='2' and frequency<='4' then 1 else null end)/count(1))*100),2),'%') as '购买2-4次占比',
concat(round(((count(case when frequency>='5' and frequency<='7' then 1 else null end)/count(1))*100),2),'%') as '购买5-7次占比',
concat(round(((count(case when frequency>='8' and frequency<='10' then 1 else null end)/count(1))*100),2),'%') as '购买8-10次占比',
concat(round(((count(case when frequency>='11' then 1 else null end)/count(1))*100),2),'%') as '购买大于11次占比',
count(case when frequency='1' then 1 else null end) as '购买1次人数',
count(case when frequency>='2' and frequency<='4' then 1 else null end) as '购买2-4次人数',
count(case when frequency>='5' and frequency<='7' then 1 else null end) as '购买5-7次人数',
count(case when frequency>='8' and frequency<='10' then 1 else null end) as '购买8-10次人数',
count(case when frequency>='11' then 1 else null end) as '购买大于11次人数',
count(1) as '总计'
from 
(select
user_log_acct,
count(parent_sale_ord_id) as frequency --每一父单的情况
from
(select 
user_log_acct,
parent_sale_ord_id
from ads_inno.yifan_wine_order_three_month
where 
substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') --剔除大宗订单
group by
user_log_acct,
parent_sale_ord_id)a 
group by user_log_acct)b


################################################建wine_order_one_month的订单表##########################################
create table ads_inno.yifan_wine_order_one_month_2 as
select 
sale_ord_id,--'销售订单编号',
sale_qtty,--'销售数量',
user_log_acct,--'用户登陆账号',
item_sku_id,--'商品SKU编号',
item_sku_name,--'SKU商品名称',
brand_cd,--'品牌名称',
item_first_cate_cd,--'商品一级分类代码',
item_second_cate_cd,--'商品二级分类代码',
item_third_cate_cd,--'商品三级分类代码',
type,--'经营模式 自营：B2C 第三方：POP',
parent_sale_ord_id,--'父销售订单编号',
after_prefr_amount,--'优惠后金额',
sale_ord_tm,--'下单时间',
free_goods_flag,--'赠品标识,1为赠品',
delv_way_cd,--'配送方式代码',
ord_flag,--'订单标识',
case 
  when item_second_cate_cd='12260' then '白酒'
  when item_second_cate_cd='14714' then '葡萄酒'
  when item_second_cate_cd='14716' then '啤酒'
  when item_second_cate_cd='14715' then '洋酒'
  when item_second_cate_cd='14717' then '黄酒/养生酒'
end as item_second_cate_name  --'二级类目名称'
from adm.adm_m04_ord_det_sum_snapshot
where 
dt >= '2019-02-01'
and dt <= '2019-02-28'
and sale_ord_dt = dt 
and sale_ord_valid_flag='1'---订单有效标识
and split_status_cd <> 1--- 1= 拆完单后父单（卡出来不需要拆分的父单和拆后的子单）防止会有父单套父单的情况
and after_sale_flag <> 1 -- 不要换货的单，换货是换新，不是买了东西不喜欢然后取消订单换货（售后订单标识，售后的情况不要，因为属于换新）
and b2c_distribute_flag <> 1 -- 自营（卡分销情况，给一些小平台的订单，不算做JD给消费者的情况）
and xintonglu_flag <> 1  -- 线下店铺  （新通路）  
----and is_deal_ord ='1' ----(1判断是成交订单，已包含sale_ord_valid_flag订单有效标识)
and item_third_cate_cd in (
'9435',
'9436',
'9437',
'9438',
'9439',
'12261',
'14743',
'14744',
'14746',
'14756',
'14757',
'15601',
'15602',
'15604',
'15605',
'15606')
###先查数据量
select count(1) from ads_inno.yifan_wine_order_one_month_2   -----2875913


##No.34&35
###下单日期偏好和下单时间段偏好
#提数逻辑：单位：父订单，因为需要看每一笔父单的下单情况，所以不需要去重。 
#1.计算出下单的时间点和下单日期，2.计算该下单时间点和时间段的数据量，和百分比。
select
concat(round(((count(case when weekday >'0' and weekday <'6' then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '工作日下单',
concat(round(((count(case when weekday ='0' or weekday ='6' then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '周末下单',
concat(round(((count(case when time >=6 and time <8 then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '早上下单',
concat(round(((count(case when time >=8 and time <12 then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '上午下单',
concat(round(((count(case when time >=12 and time <14 then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '中午下单',
concat(round(((count(case when time >=14 and time <18 then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '下午下单',
concat(round(((count(case when time >=18 and time <24 then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '晚上下单',
concat(round(((count(case when time >=0 and time <6 then 1  else null end)/count(parent_sale_ord_id))*100),2),'%') as '凌晨下单',
cast(count(case when weekday >'0' and weekday <'6' then 1  else null end) as int) as '工作日下单数据量',
cast(count(case when weekday ='0' or weekday ='6' then 1  else null end) as int) as '周末下单数据量',
cast(count(case when time >=6 and time <8 then 1  else null end) as int) as '早上下单数据量',
cast(count(case when time >=8 and time <12 then 1  else null end) as int) as '上午下单数据量',
cast(count(case when time >=12 and time <14 then 1  else null end) as int) as '中午下单数据量',
cast(count(case when time >=14 and time <18 then 1  else null end) as int) as '下午下单数据量',
cast(count(case when time >=18 and time <24 then 1  else null end) as int) as '晚上下单数据量',
cast(count(case when time >=0 and time <6 then 1  else null end) as int) as '凌晨下单数据量',
cast(count(1) as int) as '总计'
from
(select
parent_sale_ord_id,
cast(substr(sale_ord_tm,12,2) as int) as time,  ---计算出下单时间点
pmod(datediff(substr(sale_ord_tm, 1, 10), '2018-01-01') -6, 7) as weekday  ---计算出下单的日期
from 
(select
parent_sale_ord_id,
sale_ord_tm
from ads_inno.yifan_wine_order_one_month_2
where   
substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') ---剔除大宗订单
group by 
parent_sale_ord_id,
sale_ord_tm)a)b  


#NO.37
###最近一个月订单数分布
#提数逻辑：单位：父订单，
#1.计算每个user_log_acct的购买次数，以父订单为单位（count(parent_sale_ord_id)），需要看每一单的情况，所以不需要去重。 
#2.计算每种购买次数的数据量和百分比。
select  
concat(round(((count(case when frequency='1' then 1 else null end)/count(1))*100),2),'%') as '购买一次占比',
concat(round(((count(case when frequency='2' then 1 else null end)/count(1))*100),2),'%') as '购买2次占比',
concat(round(((count(case when frequency='3' then 1 else null end)/count(1))*100),2),'%') as '购买3次占比',
concat(round(((count(case when frequency='4' or frequency='5' then 1 else null end)/count(1))*100),2),'%') as '购买4-5次占比',
concat(round(((count(case when frequency>='6' then 1 else null end)/count(1))*100),2),'%') as '购买6次以上占比',
count(case when frequency='1' then 1 else null end) as '购买1次的人数',
count(case when frequency='2' then 1 else null end) as '购买2次的人数',
count(case when frequency='3' then 1 else null end) as '购买3次的人数',
count(case when frequency='4' or frequency='5' then 1 else null end) as '购买4-5次的人数',
count(case when frequency>='6' then 1 else null end) as '购买6次以上的人数',
count(1) as '总计'
from 
(select
user_log_acct,
count(parent_sale_ord_id) as frequency  --计算出每一父单的数据量
from
(select 
user_log_acct,
parent_sale_ord_id
from ads_inno.yifan_wine_order_one_month_2
where 
substring(ord_flag, 40, 1) not in ('1','2','3','4','5','6','7')  --剔除大宗订单
group by
user_log_acct,
parent_sale_ord_id)a 
group by user_log_acct)b 


#No.39
###最近一个月平均客单件分布
#提数逻辑：单位：父订单，1.算出每一个父订单下的金额（sum(after_prefr_amount)），2.计算每一种客单价区间的的数据量和占比
select  
concat(round(((count(case when Money>0 and Money<=399 then 1 else null end)/count(1))*100),2),'%') as '客单价在0~399之间占比',
concat(round(((count(case when Money>399 and Money<=799 then 1 else null end)/count(1))*100),2),'%') as '客单价在399~799之间占比',
concat(round(((count(case when Money>799 and Money<=1199 then 1 else null end)/count(1))*100),2),'%') as '客单价在799~1199之间占比',
concat(round(((count(case when Money>1199 then 1 else null end)/count(1))*100),2),'%') as '客单价大于1199占比',
count(case when Money>0 and Money<=399 then 1 else null end) as '0~399数据量',
count(case when Money>399 and Money<=799 then 1 else null end) as '399~799数据量',
count(case when Money>799 and Money<=1199 then 1 else null end) as '799~1199数据量',
count(case when Money>1199 then 1 else null end) as '大于1199数据量',
count(1) as '总计'
from 
(select 
user_log_acct,
parent_sale_ord_id,
cast(sum(after_prefr_amount) as int) as Money ---计算每一父订单的金额，转换成int形式
from ads_inno.yifan_wine_order_one_month_2
where 
substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') --剔除大宗订单
and after_prefr_amount >0  --选取的优惠后金额为大于0的，因优惠后金额会有负值
group by
user_log_acct,
parent_sale_ord_id)a 


#No.32
###送货方式分布
#提数逻辑：单位：父订单，1.确认每种送货方式，2.计算每种送货方式的数据量和百分比
select 
concat(round(((count(case when delv_way_cd_new ='1' then 1  else null end)/count(1))*100),2),'%') as '自提',
count(case when delv_way_cd_new ='1' then 1  else null end) as '自提数据量',
concat(round(((count(case when delv_way_cd_new ='0' then 1  else null end)/count(1))*100),2),'%') as '非自提',
count(case when delv_way_cd_new ='0' then 1  else null end) as '非自提数据量',
concat(round(((count(case when delv_way_cd_new ='-1' then 1  else null end)/count(1))*100),2),'%') as '未识别',
count(case when delv_way_cd_new ='-1' then 1  else null end) as '未识别数据量',
count(1) as '总计'
from
(select
parent_sale_ord_id,
delv_way_cd_new
from
(select 
parent_sale_ord_id,
case 
   when delv_way_cd='64' then '1'
   when delv_way_cd='-1' then '-1'
   else '0'
end as delv_way_cd_new  --确定送货方式
from ads_inno.yifan_wine_order_one_month_2
where substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7'))a --剔除大宗订单
group by 
parent_sale_ord_id,
delv_way_cd_new)b


#No.10
###有无赠品分布
#提数逻辑：单位：人数，计算每种分类的数据量和百分比
select 
concat(round(((count(case when free_goods_flag ='1' then 1  else null end)/count(1))*100),2),'%')  as '无赠品占比',
concat(round(((count(case when free_goods_flag ='0' then 1  else null end)/count(1))*100),2),'%') as '有赠品占比',
concat(round(((count(case when free_goods_flag = 'null' then 1 else null end)/count(1))*100),2),'%') as '未识别',
count(case when free_goods_flag ='1' then 1  else null end) as '有赠品人数',
count(case when free_goods_flag ='0' then 1  else null end) as '无赠品人数',
count(case when free_goods_flag = 'null' then 1 else null end) as '未识别',
count(1) as '总计'
from
(select
free_goods_flag,
user_log_acct
from ads_inno.yifan_wine_order_one_month_2
where substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') --剔除大宗订单
group by 
free_goods_flag,
user_log_acct)a


#No.11, 21-25
###购买频次分布
#提数逻辑：单位：人数，1.计算在每一种酒类维度下（item_second_cate_name），每一个user_log_acct的去重后父订单的次数，2.计算每种购买次数的数据量和百分比
select 
item_second_cate_name as '酒类',
concat(round(((count(case when freq = '1'then 1 else null end)/ count(freq))*100),2),'%') as '购买1次占比',
concat(round(((count(case when freq = '2' then 1 else null end)/ count(freq))*100),2),'%') as '购买2次占比',
concat(round(((count( case when freq >= '3' then 1 else null end )/ count(freq))*100),2),'%') as '购买3次以上占比',
count(case when freq = '1'then 1 else null end) as '购买1次的人数',
count(case when freq = '2'  then 1 else null end) as '购买2次的人数',
count( case when freq >= '3' then 1 else null end ) as '购买3次以上的人数',
count (freq) as '数据量'
from (select
	user_log_acct,
	item_second_cate_name,
	count(distinct parent_sale_ord_id) as freq --算去重后的每个父订单的数
from
	ads_inno.yifan_wine_order_one_month_2
where
	item_second_cate_cd in ('12260', '14714', '14715', '14716', '14717') --所有的五种酒类的二级类目
   and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') --剔除大宗订单
group by
	user_log_acct,
	item_second_cate_name) a  
group by 
item_second_cate_name 


##################################一个月的主品牌表wine_order_one_month_main_brand_id#######################################
create table ads_inno.yifan_wine_order_one_month_main_brand_id_2 as
select
sale_ord_id,--'销售订单编号',
sale_qtty,--'销售数量',
user_log_acct,--'用户登陆账号',
a.item_sku_id,--'商品SKU编号',
item_sku_name,--'SKU商品名称',
brand_cd,--'品牌名称',
item_first_cate_cd,--'商品一级分类代码',
item_second_cate_cd,--'商品二级分类代码',
item_third_cate_cd,--'商品三级分类代码',
type,--'经营模式 自营：B2C 第三方：POP',
parent_sale_ord_id,--'父销售订单编号',
after_prefr_amount,--'优惠后金额',
main_brand_id, --'主品牌编号',
brand_group_name，--'品牌名称',
ord_flag --'订单标识'
from
	ads_inno.yifan_wine_order_one_month_2 a
join
	(
		select
			brand_id,
			main_brand_id,
			brand_group_name
		from
			fdm.fdm_forest_brands_chain
		where
			dp = 'ACTIVE'
	)
	b
on
	a.brand_cd = b.brand_id

#计算数据量
select count(1) from ads_inno.yifan_wine_order_one_month_main_brand_id_2  ----	2870515


#No.15,26-30
###品牌偏好分布
##父订单
#提数逻辑：单位：父订单 1.计算每个二级类目下每个品牌（brand_group_name）去重后的父订单数，生成表a
#2.计算每个二级类目下的去重后的父订单数，生成表b
#3.表a和表b进行关联，
#4.计算每个品牌在该酒类下的百分比（每个品牌的父订单数/对应的二级类目的总父订单数），在二级类目下按表a的结果进行倒序排序，
#5.提取每种酒类的top5
select
	g.item_second_cate_name as '酒类',
	g.brand_group_name as '品牌',
	g.data as '数据量',
	g.property as '百分比',
	g.rank as '排名'
from
	(
		select
			a.data,
			a.item_second_cate_name,
			a.brand_group_name,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property, --计算每个品牌的百分比
			row_number() over(partition by a.item_second_cate_name order by a.data desc) as rank --在二级类目的分区下对计算的品牌数据倒序排序
		from
			(
				select
					count(distinct parent_sale_ord_id) as data, --去重后的父订单
					item_second_cate_name,
					brand_group_name
				from
					ads_inno.yifan_wine_order_one_month_main_brand_id_2
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717') --二级类目名称
					  and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') --剔除大宗订单
				group by
					item_second_cate_name,
					brand_group_name
				order by
					item_second_cate_name,
					data desc
			)
			a
		left join --关联a,b表
			(
				select
					item_second_cate_name,
					count(distinct parent_sale_ord_id) as total
				from
					ads_inno.yifan_wine_order_one_month_main_brand_id_2
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') --剔除大宗订单
				group by
					item_second_cate_name
				order by
					item_second_cate_name,
					total desc
			)
			b
        on a.item_second_cate_name = b.item_second_cate_name --关联条件是二级类目名称
		group by
			a.data,
			a.item_second_cate_name,
			a.brand_group_name,
			b.total
	)
	g
where
	rank <= 5 --提取每种酒类的top5


##人数
#提数逻辑：单位：人数，具体逻辑参考品牌偏好分布-父订单
select
	g.item_second_cate_name as '酒类',
	g.brand_group_name as '品牌',
	g.data as '数据量',
	g.property as '百分比',
	g.rank as '排名'
from
	(
		select
			a.data,
			a.item_second_cate_name,
			a.brand_group_name,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property,
			row_number() over(partition by a.item_second_cate_name order by a.data desc) as rank
		from
			(
				select
					count(distinct user_log_acct) as data,
					item_second_cate_name,
					brand_group_name
				from
					ads_inno.yifan_wine_order_one_month_main_brand_id_2
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					  and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
				group by
					item_second_cate_name,
					brand_group_name
				order by
					item_second_cate_name,
					data desc
			)
			a
		left join
			(
				select
					item_second_cate_name,
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_one_month_main_brand_id_2
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					  and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
				group by
					item_second_cate_name
				order by
					item_second_cate_name,
					total desc
			)
			b
        on a.item_second_cate_name = b.item_second_cate_name
		group by
			a.data,
			a.item_second_cate_name,
			a.brand_group_name,
			b.total
	)
	g
where
	rank <= 5


##金额
#提数逻辑：单位：金额，具体逻辑参考品牌偏好分布-父订单
select
	g.item_second_cate_name as '酒类',
	g.brand_group_name as '品牌',
	g.data as '数据量',
	g.property as '百分比',
	g.rank as '排名'
from
	(
		select
			a.data,
			a.item_second_cate_name,
			a.brand_group_name,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property,
			row_number() over(partition by a.item_second_cate_name order by a.data desc) as rank
		from
			(
				select
					sum(after_prefr_amount) as data,
					item_second_cate_name,
					brand_group_name
				from
					ads_inno.yifan_wine_order_one_month_main_brand_id_2
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					  and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
				group by
					item_second_cate_name,
					brand_group_name
				order by
					item_second_cate_name,
					data desc
			)
			a
		left join
			(
				select
					item_second_cate_name,
					sum(after_prefr_amount) as total
				from
					ads_inno.yifan_wine_order_one_month_main_brand_id_2
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					  and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
				group by
					item_second_cate_name
				order by
					item_second_cate_name,
					total desc
			)
			b
        on a.item_second_cate_name = b.item_second_cate_name
		group by
			a.data,
			a.item_second_cate_name,
			a.brand_group_name,
			b.total
	)
	g
where
	rank <= 5


################################################一个月的user表wine_order_user_one_month#################################################
create table ads_inno.yifan_wine_order_user_one_month as
select
sale_ord_id,--'销售订单编号',
sale_qtty,--'销售数量',
a.user_log_acct,--'用户登陆账号',
item_sku_id,--'商品SKU编号',
item_sku_name,--'SKU商品名称',
brand_cd,--'品牌名称',
item_first_cate_cd,--'商品一级分类代码',
item_second_cate_cd,--'商品二级分类代码',
item_third_cate_cd,--'商品三级分类代码',
type,--'经营模式 自营：B2C 第三方：POP',
parent_sale_ord_id,--'父销售订单编号',
after_prefr_amount,--'优惠后金额',
sale_ord_tm,--'下单时间',
free_goods_flag,--'赠品标识',
delv_way_cd,--'配送方式代码',
ord_flag,--'订单标识',
item_second_cate_name,--'二级类目名称',
cpp_addr_city,--'常用收货市名称',
cpp_base_age,--'用户年龄',
cpp_base_sex,--'性别',
csf_sale_client,--'最常用下单路径',
cpp_base_ulevel,--'用户级别',
cpp_base_profession,--'级别',
cpp_base_education,--'学历',
cfv_cate_30dcate1,--'最近30天的订单量最大一级品类',
cfv_sens_comment,--'用户评价敏感度(关注商品评价模型)'
cfv_sens_promotion,--'用户促销敏感度(促销敏感度模型)'
cfv_cate_90dcate1,--'最近90天的订单量最大一级品类'
biggest_category,--30dcate中第一个类目
category_association--90dcate中第一个类目
from 
(select
sale_ord_id ,
sale_qtty,
user_log_acct ,
item_sku_id,
item_sku_name,
brand_cd,
item_first_cate_cd,
item_second_cate_cd,
item_third_cate_cd,
type,
parent_sale_ord_id,
after_prefr_amount,
sale_ord_tm,
free_goods_flag,
delv_way_cd,
ord_flag,
item_second_cate_name
from 
ads_inno.yifan_wine_order_one_month_2)a
left join 
(select
user_log_acct,
cpp_addr_city,
cpp_base_age ,
cpp_base_sex,
csf_sale_client,
cpp_base_ulevel,
cpp_base_profession,
cpp_base_education,
cfv_cate_30dcate1,
case 
   when cfv_cate_30dcate1='-1' then cfv_cate_30dcate1
   when cfv_cate_30dcate1 is null or lower(cfv_cate_30dcate1)='null' or cfv_cate_30dcate1='' then 'NULL'
   when substr(cfv_cate_30dcate1,-2)='##' then split (cfv_cate_30dcate1,'\\##')[0]
   else split (cfv_cate_30dcate1,'\\#')[0] 
   end as biggest_category,
case
  when cfv_sens_comment='-1' then cfv_sens_comment
  when cfv_sens_comment is null or lower(cfv_sens_comment)='null' or cfv_sens_comment='' then 'NULL'
  else substr(cfv_sens_comment,4)
  end as cfv_sens_comment,
case
  when cfv_sens_promotion='-1' then cfv_sens_promotion
  when cfv_sens_promotion is null or lower(cfv_sens_promotion)='null' or cfv_sens_promotion='' then 'NULL'
  else substr(cfv_sens_promotion,4)
  end as cfv_sens_promotion,
cfv_cate_90dcate1,
case 
   when cfv_cate_90dcate1='-1' then cfv_cate_90dcate1
   when cfv_cate_90dcate1 is null or lower(cfv_cate_90dcate1)='null' or cfv_cate_90dcate1='' then 'NULL'
   when substr(cfv_cate_90dcate1,-2)='##' then split (cfv_cate_90dcate1,'\\##')[0]
   else split (cfv_cate_90dcate1,'\\#')[0] 
   end as category_association
from app.app_ba_userprofile_prop_nonpolar_view_ext
where dt ='ACTIVE')b
on a.user_log_acct=b.user_log_acct

###数据量和yifan_wine_order_one_month_2数据量是一样的 ---2875913


#No.1
###常用收货地址所在城市分布
#提数逻辑：单位：人数，1.计算每个城市下（cpp_addr_city）去重后的user_log_acct的数据量，生成表a
#2.计算表中总共的去重后的user_log_acct的数据量，生成表b
#3.表a和表b进行关联，
#4.计算每个城市的百分比（每个城市的数据量/所有城市的总量），并以百分比进行倒序排序
select
	g.cpp_addr_city as '常用收货地址所在城市',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cpp_addr_city,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property --计算百分比
		from
			(
				select
					cpp_addr_city,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cpp_addr_city
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1 --关联后a和b表恒相等
		order by
			cast(regexp_replace(property, '%', '') as double) desc, --以百分比倒序排序，因为百分比是字符串的形式，在排序时会出现乱序情况，所以用regexp_replace把百分比中的%代替为‘’,之后用cast把字符串转换成double型，进行倒序排序。
			data desc
	)
	g


#No.2
#####年龄段分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	g.age as '年龄',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cpp_base_age as age,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					cpp_base_age,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cpp_base_age
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.3
###性别分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	g.sex as '性别',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cpp_base_sex as sex,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					cpp_base_sex,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cpp_base_sex
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.4
###渠道分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	g.channel as '渠道',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.csf_sale_client as channel,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					csf_sale_client,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					csf_sale_client
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.5
###会员等级分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	g.level as '会员等级',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cpp_base_ulevel as level,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					cpp_base_ulevel,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cpp_base_ulevel
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.6
###职业分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	case
		when g.profession = 'd'
		then '白领/一般职员'
		when g.profession = 'f'
		then '教师'
		when g.profession = 'g'
		then '农民'
		when g.profession = 'e'
		then '工人/服务业人员'
		when g.profession = 'c'
		then '公务员/事业单位'
		when g.profession = 'h'
		then '学生'
		when g.profession = 'b'
		then '医务人员'
		when g.profession = 'a'
		then '金融从业者'
		 else '未识别'
	end as '职业',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cpp_base_profession as profession,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					cpp_base_profession,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cpp_base_profession
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.6
###教育水平
#提数逻辑：参考-常用收货地址所在城市分布
select
	case 
	when g.education = '3' then '大学(专科及本科)'
	when g.education = '2' then '高中(中专)'
	when g.education = '4' then '研究生(硕士及以上)'
	when g.education = '1' then '初中及以下'
	else '未识别' 
	end as '教育水平',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cpp_base_education as education,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					cpp_base_education,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cpp_base_education
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.31
###评论敏感度分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	case
		when g.comment = '5'
		then '极度敏感'
		when g.comment = '4'
		then '高度敏感'
		when g.comment = '3'
		then '中度敏感'
		when g.comment = '2'
		then '轻度敏感'
		when g.comment = '1'
		then '不敏感'
		else '未识别'
	end as '评论敏感度',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cfv_sens_comment as comment,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					cfv_sens_comment,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cfv_sens_comment
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.36
###促销敏感度分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	case
		when g.promotion = '5'
		then '极度敏感'
		when g.promotion = '4'
		then '高度敏感'
		when g.promotion = '3'
		then '中度敏感'
		when g.promotion = '2'
		then '轻度敏感'
		when g.promotion = '1'
		then '不敏感'
		else '未识别'
	end as '促销敏感度',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.cfv_sens_promotion as promotion,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					cfv_sens_promotion,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					cfv_sens_promotion
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.8
###消费大类分布
#提数逻辑：参考-常用收货地址所在城市分布
select
	g.biggest_category as '消费大类',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.biggest_category,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					biggest_category,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					biggest_category
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g


#No.40
###品类关联
#提数逻辑：参考-常用收货地址所在城市分布
select
	g.category_association as '品类相关',
	g.data as '数据量',
	g.total as '总计',
	g.property as '百分比'
from
	(
		select
			a.category_association,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property
		from
			(
				select
					category_association,
					count(distinct user_log_acct) as data
				from
					ads_inno.yifan_wine_order_user_one_month
				group by
					category_association
			)
			a
		left join
			(
				select
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_user_one_month
			)
			b
		on
			1 = 1
		order by
			cast(regexp_replace(property, '%', '') as double) desc,
			data desc
	)
	g



########################################合并附加属性信息表wine_order_attribute_one_month######################################
create table ads_inno.yifan_wine_order_attribute_one_month as
select
sale_ord_id,--'销售订单编号',
sale_qtty,--'销售数量',
user_log_acct,--'用户登陆账号',
a.item_sku_id,--'商品SKU编号',
item_sku_name,--'SKU商品名称',
brand_cd,--'品牌名称',
item_first_cate_cd,--'商品一级分类代码',
item_second_cate_cd,--'商品二级分类代码',
item_third_cate_cd,--'商品三级分类代码',
type,--'经营模式 自营：B2C 第三方：POP',
parent_sale_ord_id,--'父销售订单编号',
after_prefr_amount,--'优惠后金额',
sale_ord_tm,--'下单时间',
free_goods_flag,--'赠品标识,1为赠品',
delv_way_cd,--'配送方式代码',
ord_flag,--'订单标识',
item_second_cate_name,--'二级类目名称'
package,--包装
flavor,---香型
degree,---度数
capacity --- 容量 
from 
(select
sale_ord_id,
sale_qtty,
user_log_acct,
item_sku_id,
item_sku_name,
brand_cd,
item_first_cate_cd,
item_second_cate_cd,
item_third_cate_cd,
type,
parent_sale_ord_id,
after_prefr_amount,
sale_ord_tm,
free_goods_flag,
delv_way_cd,
ord_flag,
item_second_cate_name
from 
ads_inno.yifan_wine_order_one_month_2)a
left join 
(select
item_sku_id,
max(a4) as package,  ---包装
max(a1) as flavor,   ---香型
max(a3) as degree,   ---度数
max(a2) as capacity  ---容量
from(
select 
item_sku_id,
case 
   when com_attr_cd='3221'and (com_attr_value_name is not null and com_attr_value_name != 'NULL' and com_attr_value_name != '' )   --  3221香型
   then com_attr_value_name
   else 0 
end as a1,
case 
  when com_attr_cd='1948'and (com_attr_value_name is not null and com_attr_value_name != 'NULL' and com_attr_value_name != '' )  --   1948容量
  then com_attr_value_name
  else 0 
end as a2,
case 
  when com_attr_cd='3655'and (com_attr_value_name is not null and com_attr_value_name != 'NULL' and com_attr_value_name != '' )   --3655酒精度数
  then com_attr_value_name
  else 0 
  end as a3,
case
  when com_attr_cd='2762'and com_attr_value_cd in ('32941','71141') and (com_attr_value_name is not null and com_attr_value_name != 'NULL' and com_attr_value_name != '' )---卡出来礼品包装的商品
  then com_attr_value_name
  else 0 
  end as a4
from gdm.gdm_m03_item_sku_ext_attr_da
where dt='2019-01-01'
and cate_id in(
'9435',
'9436',
'9437',
'9438',
'9439',
'12261',
'14743',
'14744',
'14746',
'14756',
'14757',
'15601',
'15602',
'15604',
'15605',
'15606'
))b
group by item_sku_id)c
on a.item_sku_id=c.item_sku_id
##数据量2875913


#No.9
###酒是否礼品包装分布
#提数逻辑：单位：人数，1.计算在二级类目下每个package的去重后的user_log_acct数据量，生成表a，
#2.计算去重后的user_log_acct在每个二级类目下的数据量，生成表b
#3.表a和表b进行关联，
#4.计算每种package在该酒类下的百分比（每个package的数据量/对应的二级类目的总量），在二级类目下按表a的结果进行倒序排序
select
	g.item_second_cate_name as '酒类',
	g.package as '是否礼品包装',
	g.data as '数据量',
	g.property as '百分比'
from
	(
		select
			a.item_second_cate_name,
			a.package,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property, --计算百分比
            row_number() over(partition by a.item_second_cate_name order by a.data desc) as rank --在二级类目的分区下对计算的package数据量进行倒序排序
		from
			(
				select
					item_second_cate_name,
					package,
					count(distinct user_log_acct) as data 
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717') --二级类目下的五种酒类（白酒，葡萄酒，洋酒，啤酒，黄酒/养生酒）
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7') --剔除大宗订单
				group by
					item_second_cate_name,
					package
              order by
					item_second_cate_name,
					data desc
			)
			a
		left join
			(
				select
                    item_second_cate_name,  
					count(distinct user_log_acct) as total
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
              group by item_second_cate_name
              order by
					item_second_cate_name,
					total desc
			)
			b
		on
			a.item_second_cate_name = b.item_second_cate_name --以二级类目名称进行关联
		group by
			a.data,
			a.item_second_cate_name,
			a.package,
			b.total
	)
	g


#No.12
###香型偏好分布
#提数逻辑：单位：人次，具体逻辑参考-酒是否礼品包装分布
	g.item_second_cate_name as '酒类',
	g.flavor as '香型',
	g.data as '数据量',
	g.property as '百分比'
from
	(
		select
			a.item_second_cate_name,
			a.flavor,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property,
            row_number() over(partition by a.item_second_cate_name order by a.data desc) as rank
		from
			(
				select
					item_second_cate_name,
					flavor,
					count(parent_sale_ord_id) as data
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
				group by
					item_second_cate_name,
					flavor
              order by
					item_second_cate_name,
					data desc
			)
			a
		left join
			(
				select
                    item_second_cate_name,  
					count(parent_sale_ord_id) as total
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
              group by item_second_cate_name
              order by
					item_second_cate_name,
					total desc
			)
			b
		on
			a.item_second_cate_name = b.item_second_cate_name
		group by
			a.data,
			a.item_second_cate_name,
			a.flavor,
			b.total
	)
	g


#No.13
###度数偏好分布
#提数逻辑：单位：人次，具体逻辑参考-酒是否礼品包装分布
select
	g.item_second_cate_name as '酒类',
	g.degree as '度数',
	g.data as '数据量',
	g.property as '百分比'
from
	(
		select
			a.item_second_cate_name,
			a.degree,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property,
            row_number() over(partition by a.item_second_cate_name order by a.data desc) as rank
		from
			(
				select
					item_second_cate_name,
					degree,
					count(parent_sale_ord_id) as data
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
				group by
					item_second_cate_name,
					degree
              order by
					item_second_cate_name,
					data desc
			)
			a
		left join
			(
				select
                    item_second_cate_name,  
					count(parent_sale_ord_id) as total
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
              group by item_second_cate_name
              order by
					item_second_cate_name,
					total desc
			)
			b
		on
			a.item_second_cate_name = b.item_second_cate_name
		group by
			a.data,
			a.item_second_cate_name,
			a.degree,
			b.total
	)
	g



#No.14
###规格偏好分布
#提数逻辑：单位：人次，具体逻辑参考-酒是否礼品包装分布
select
	g.item_second_cate_name as '酒类',
	g.capacity as '规格',
	g.data as '数据量',
	g.property as '百分比'
from
	(
		select
			a.item_second_cate_name,
			a.capacity,
			a.data,
			b.total,
			concat(round(((a.data / b.total) * 100), 2), '%') as property,
            row_number() over(partition by a.item_second_cate_name order by a.data desc) as rank
		from
			(
				select
					item_second_cate_name,
					capacity,
					count(parent_sale_ord_id) as data
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
				group by
					item_second_cate_name,
					capacity
              order by
					item_second_cate_name,
					data desc
			)
			a
		left join
			(
				select
                    item_second_cate_name,  
					count(parent_sale_ord_id) as total
				from
					ads_inno.yifan_wine_order_attribute_one_month
				where
					item_second_cate_cd in('12260', '14715', '14716', '14714', '14717')
					and substring(ord_flag, 40, 1) not in ('1','2','3', '4','5','6','7')
              group by item_second_cate_name
              order by
					item_second_cate_name,
					total desc
			)
			b
		on
			a.item_second_cate_name = b.item_second_cate_name
		group by
			a.data,
			a.item_second_cate_name,
			a.capacity,
			b.total
	)
	g





