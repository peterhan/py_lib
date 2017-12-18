-- c2线索总数
-- explain 
SELECT regexp_replace('${hivevar:dt}','-','') report_dt,'-9' AS platform,leads_type,'C2' AS kpi_type, sum(xs) c2
FROM
  ( ------询价订单
SELECT '1' AS leads_type,count(leads_id) xs -- FROM gdm.gdm_sales_leads_day_detail

   FROM mds.mds_usc_sales_leads_old_info
   WHERE dt = '${hivevar:dt}'
     AND leads_type IN (102,104,201,208,301,304,501)
     AND to_date(input_time)='2017-12-16'
     UNION ALL ----400电话线索

     SELECT '2' AS leads_type,
            count(phone) xs -- FROM gdm.gdm_sales_leads_phone_inc WHERE dt='${hivevar:dt}'

     FROM mds.mds_usc_sales_leads_phone_info WHERE dt='${hivevar:dt}'
     AND leads_type =400
     AND to_date(leads_create_time)='${hivevar:dt}'
     UNION ALL -----170电话线索

     SELECT '2' AS leads_type,
            count(cccdrid) xs -- FROM bdm.bdm_usedcardealer_callcentercalldetailrecord_inc WHERE dt='${hivevar:dt}'

     FROM stage.s_u04_callcentercalldetailrecord WHERE dt='${hivevar:dt}'
     AND fromtype=1
     AND to_date(inserttime)='${hivevar:dt}'
     AND cccdrstatus=0
     UNION ALL ------消费贷

     SELECT '3' AS leads_type,
            count(carfinancialorderno) xs -- FROM bdm.bdm_usedcar_financial_order WHERE substr(createtime,1,10) = '${hivevar:dt}'

     FROM stage.s_u03_financialorder WHERE substr(createtime,1,10) = '${hivevar:dt}'
     AND dt = '${hivevar:dt}'
     UNION ALL -----抢先开

     SELECT '3' AS leads_type,count(1) xs
     FROM
       (SELECT DISTINCT t5.ordercode r
        FROM
          (SELECT t1.*,t2.cityid_s
           FROM
             (SELECT * -- FROM bdm.bdm_usedcar_rentalleads

              FROM stage.s_u03_rentalleads
              WHERE substr(createtime,1,10)='${hivevar:dt}'
                AND status = 0 ) t1
           LEFT JOIN
             (SELECT dealer_id,
                     city_id AS cityid_s -- FROM dim.dim_dealers

              FROM mds.mds_usc_dealer_info
              WHERE dt = '${hivevar:dt}' ) t2 ON t1.dealerid = t2.dealer_id) t5
        JOIN
          (SELECT cid -- FROM bdm.bdm_usedcar_rentalopencity

           FROM stage.s_u03_rentalopencity
           WHERE isdel <> 1
             UNION ALL
             SELECT cid -- FROM bdm.bdm_usedcar_rentalopencitysurroundingcities WHERE dt ='${hivevar:dt}'

             FROM stage.s_u03_rentalopencitysurroundingcities WHERE isdel <> 1 ) t6 ON t5. cityid_s = t6.cid
        UNION ALL SELECT DISTINCT cccdrid r
        FROM
          (SELECT h.dt,h.cityid_s,h.cccdrid,h.ccppnum
           FROM
             (SELECT g.dt,city_id cityid_s,cccdrid,
                     ccppnum --无锡、芜湖为虚拟城分别归到苏州和南京
 -- FROM bdm.bdm_usedcardealer_callcentercalldetailrecord_inc g

              FROM stage.s_u04_callcentercalldetailrecord g
              JOIN mds.mds_usc_dealer_info d ON g.dealerid=d.dealer_id
              AND d.dt= '${hivevar:dt}'
              WHERE g.dt='${hivevar:dt}'
                AND to_date(g.inserttime)='${hivevar:dt}'
                AND g.fromtype=0 ) h
           JOIN
             (SELECT cid -- FROM bdm.bdm_usedcar_rentalopencity

              FROM stage.s_u03_rentalopencity
              WHERE isdel <> 1
                UNION ALL
                SELECT cid -- FROM bdm.bdm_usedcar_rentalopencitysurroundingcities WHERE dt ='${hivevar:dt}'

                FROM stage.s_u03_rentalopencitysurroundingcities WHERE isdel <> 1 ) i ON h.cityid_s = i.cid) aa
        JOIN
          (SELECT ccppnum,dealerid
           FROM
             (SELECT dealerid,
                     ccppid -- FROM bdm.bdm_usedcardealer_callcenterauth400dealerrel

              FROM stage.s_u03_callcenterauth400dealerrel
              WHERE dt = '${hivevar:dt}'
                AND ccdrstatus=10 )a
           JOIN
             (SELECT ccppid,
                     ccppnum -- FROM bdm.bdm_usedcardealer_callcenter400phonepool

              FROM stage.s_u03_callcenter400phonepool
              WHERE ccppstatus = 20 )b ON a.ccppid = b.ccppid
           GROUP BY ccppnum,dealerid)bb ON aa.ccppnum = bb.ccppnum) t ) m
GROUP BY leads_type

UNION ALL


---c1线索总数
-- explain
SELECT regexp_replace('${hivevar:dt}','-','') report_dt,'-9' AS platform,'-9' AS leads_type,'C1' AS kpi_type,
       xleads.updatecar+p.all_c2b_cnt c1_leads
FROM
  (--细线索计算
 SELECT aaa.dt dt, sum(aaa.updatecar) updatecar
   FROM
     (SELECT dt,
             count(CASE
                       WHEN STATE IN (1,2,3)
                            AND substr(pass_time,0,10)=dt THEN 1
                       ELSE NULL
                   END) updatecar--更新
 -- FROM gdm.gdm_car_day_all_detail

      FROM mds.mds_usc_carinfo
      WHERE dt ='${hivevar:dt}'
        AND STATE IN (-1,0,1,2,3,4)
        AND dealer_id<>264907
        AND (from_type <1300
             OR from_type >=1400)
        AND (kind_id IS NULL
             OR kind_id=1)
      GROUP BY dt
      UNION ALL 
      SELECT dt,
       count(CASE
                 WHEN STATE IN (1,2,3)
                      AND substr(pass_time,0,10)=dt THEN 1
                 ELSE NULL
             END) updatecar--更新
 -- FROM gdm.gdm_car_day_all_detail

      FROM mds.mds_usc_carinfo
      WHERE dt = '${hivevar:dt}'
        AND STATE IN (-1,0,1,2,3,4)
        AND from_type >=1300
        AND from_type <=1400
        AND (kind_id IS NULL
             OR kind_id=1)
      GROUP BY dt) aaa
   GROUP BY aaa.dt) xleads
LEFT JOIN
  (--粗线索计算
 SELECT dt1, count(DISTINCT cclid) AS all_c2b_cnt
   FROM
     (SELECT substr(createtime,1,10) AS dt1, cclid,leadssources,cid,fullname,
             mobile -- FROM bdm.bdm_usedcar_carcaluationleads

      FROM stage.s_u03_carcaluationleads
      WHERE dt = '${hivevar:dt}'
        AND returntype NOT IN (1,2)
        AND substr(createtime,1,10)= '${hivevar:dt}' ) a
   JOIN
     (SELECT city_id cityid,city_name cityname -- FROM dim.dim_areacity

      FROM dim.dim_usc_area
      WHERE dt='${hivevar:dt}'
        AND city_id IN (110100,310100,500100,440300,420100,430100,320400,320100,320200,610100,
                       370200,410100,320500,330100,510100,440100,441900,320600,360100,320300,
                       130100,130600,130900,130200,130400,210200,220100,230100,371300,330200,
                       330400,330700,331000,330300,350100,350200,350500,440600,450100,530100,
                       520100,340100,140100,120100,370100,210100,440400,442000,441300,360700,
                       410300,330600) ) d ON a.cid=d.cityid
   GROUP BY dt1) p ON xleads.dt=p.dt1
;