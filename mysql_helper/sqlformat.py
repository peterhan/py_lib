#!coding:utf8
s="/*!50001 VIEW `ads_1111_ec_millions_agent_order_hour_view` AS select `d`.`order_id` AS `order_id`,`n`.`invite_code` AS `invite_code`,`d`.`order_create_time` AS `order_create_time`,`d`.`payment_confirm_time` AS `payment_confirm_time` from ((((`dw_data_center`.`mall_order_data` `d` join `dw_data_center`.`mall_invite_code` `n` on((`d`.`order_id` = `n`.`order_id`))) left join `dw_data_center`.`mall_order_item` `i` on((`d`.`order_id` = `i`.`order_id`))) left join (select distinct `dim_ec_test_account_list`.`user_id` AS `user_id` from `dw_data_center`.`dim_ec_test_account_list` where (`dim_ec_test_account_list`.`user_id` is not null)) `dtw1` on((`d`.`user_id` = `dtw1`.`user_id`))) left join (select distinct trim(`dim_ec_test_account_list`.`phone`) AS `phone` from `dw_data_center`.`dim_ec_test_account_list` where ((trim(`dim_ec_test_account_list`.`phone`) is not null) and (trim(`dim_ec_test_account_list`.`phone`) <> ''))) `dtw2` on((`d`.`cons_phone` = `dtw2`.`phone`))) where (((cast(`d`.`order_create_time` as date) >= '2016-11-01') and (cast(`d`.`order_create_time` as date) <= date_format(now(),'%Y-%m-%d'))) or ((cast(`d`.`payment_confirm_time` as date) >= '2016-11-01') and (cast(`d`.`payment_confirm_time` as date) <= date_format(now(),'%Y-%m-%d')) and (isnull(`d`.`parent_order_id`) or (`d`.`sub_order_type` = 1)) and (`d`.`holiday` = 126) and (`d`.`order_type` <> 13) and (not((`i`.`sku_name` like '%测试商品%'))) and isnull(`dtw1`.`user_id`) and isnull(`dtw2`.`phone`) and (`d`.`invite_type` = 1))) */;"
import re
import sqlparse
clean_pat=re.compile(r'/\*!\d+(.*)\*/(.*)')

def u2g(st):
    return st.encode('gbk')
    
def sqlformat(sql):
    m=clean_pat.match(sql)
    if m:
        sql=m.group(1)
        rsql= sqlparse.format(sql,reindent=True)
        return rsql
    else:
        return sql
    
def test():
    import glob
    for fn in glob.glob('split_db_obj/*.view.*.sql'):
        print '-'*2,fn,'-'*3,
        for l in open(fn):
            l=sqlformat(l.strip())
            print u2g(l)
    
if __name__=='__main__':
    test()