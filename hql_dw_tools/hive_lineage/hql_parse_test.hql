--- C2

SELECT ${job_today_simple_date} staticsdate, sum(xs) c2 from
  (------询价订单
SELECT count(leadid) xs
   FROM gdm.gdm_sales_leads_day_detail
   WHERE dt = '${job_date}'
     AND leadtype IN (102,104,201,208,301,304,501)
   UNION ALL ----400电话线索
 SELECT count(phone) xs
   FROM gdm.gdm_sales_leads_phone_inc
   WHERE dt='${job_date}'
     AND leadtype =400
   UNION ALL -----170电话线索
SELECT count(cccdrid) xs
   FROM bdm.bdm_usedcardealer_callcentercalldetailrecord_inc
   WHERE dt='${job_date}'
     AND fromtype=1
     AND cccdrstatus=0
   UNION ALL ------消费贷
SELECT count(carfinancialorderno) xs
   FROM bdm.bdm_usedcar_financial_order
   WHERE substr(createtime,1,10) = '${job_date}'
     AND dt = '${job_date}'
   UNION ALL -----抢先开
SELECT count(1) xs
   FROM
     (SELECT DISTINCT t5.ordercode r
      FROM
        (SELECT t1.*,t2.cityid_s
         FROM
           (SELECT *
            FROM bdm.bdm_usedcar_rentalleads
            WHERE dt = '${job_date}'
              AND substr(createtime,1,10)='${job_date}'
              AND status = 0 ) t1
         LEFT JOIN
           (SELECT dealerid,cityid AS cityid_s
            FROM dim.dim_dealers
            WHERE dt = '${job_date}' ) t2 ON t1.dealerid = t2.dealerid ) t5
      JOIN
        (SELECT cid
         FROM bdm.bdm_usedcar_rentalopencity
         WHERE dt ='${job_date}'
           AND isdel <> 1
         UNION ALL SELECT cid
         FROM bdm.bdm_usedcar_rentalopencitysurroundingcities
         WHERE dt ='${job_date}'
           AND isdel <> 1 ) t6 ON t5. cityid_s = t6.cid
      UNION ALL SELECT DISTINCT cccdrid r
      FROM
        (SELECT h.dt,h.cityid_s,h.cccdrid,h.ccppnum
         FROM
           (SELECT g.dt,cityid cityid_s,cccdrid,ccppnum --无锡、芜湖为虚拟城分别归到苏州和南京

            FROM bdm.bdm_usedcardealer_callcentercalldetailrecord_inc g
            JOIN dim.dim_dealers d ON g.dealerid=d.dealerid
            AND d.dt= '${job_date}'
            WHERE g.dt='${job_date}'
              AND g.fromtype=0 ) h
         JOIN
           (SELECT cid
            FROM bdm.bdm_usedcar_rentalopencity
            WHERE dt ='${job_date}'
              AND isdel <> 1
            UNION ALL SELECT cid
            FROM bdm.bdm_usedcar_rentalopencitysurroundingcities
            WHERE dt ='${job_date}'
              AND isdel <> 1 ) i ON h.cityid_s = i.cid) aa
      JOIN
        (SELECT ccppnum,dealerid
         FROM
           (SELECT dealerid,ccppid
            FROM bdm.bdm_usedcardealer_callcenterauth400dealerrel
            WHERE dt = '${job_date}'
              AND ccdrstatus=10 )a
         JOIN
           (SELECT ccppid,ccppnum
            FROM bdm.bdm_usedcardealer_callcenter400phonepool
            WHERE dt = '${job_date}'
              AND ccppstatus = 20 )b ON a.ccppid = b.ccppid
         GROUP BY ccppnum,dealerid )bb ON aa.ccppnum = bb.ccppnum) t) m;

-- C1-----------------------------------------------------------------------------------------------------------------

SELECT xleads.dt, xleads.updatecar+p.all_c2b_cnt c1_leads,---c1线索总数
FROM
  (--细线索计算
 SELECT aaa.dt dt, sum(aaa.updatecar) updatecar
   FROM
     (SELECT dt,
             count(CASE
                       WHEN state IN (1,2,3)
                            AND substr(passdate,0,10)=dt THEN 1
                       ELSE NULL
                   END) updatecar--更新

      FROM gdm.gdm_car_day_all_detail
      WHERE dt BETWEEN '${job_date1}' AND '${job_date2}'
        AND state IN (-1,0,1,2,3,4)
        AND dealerid<>264907
        AND (fromtype <1300
             OR fromtype >=1400)
        AND (kindid IS NULL
             OR kindid=1)
      GROUP BY dt
      UNION ALL SELECT dt,
                       count(CASE
                                 WHEN state IN (1,2,3)
                                      AND substr(passdate,0,10)=dt THEN 1
                                 ELSE NULL
                             END) updatecar--更新

      FROM gdm.gdm_car_day_all_detail
      WHERE dt BETWEEN '${job_date1}' AND '${job_date2}'
        AND state IN (-1,0,1,2,3,4)
        AND fromtype >=1300
        AND fromtype <=1400
        AND (kindid IS NULL
             OR kindid=1)
      GROUP BY dt ) aaa
   GROUP BY aaa.dt) xleads
LEFT JOIN
  (--粗线索计算
 SELECT dt1, count(DISTINCT cclid) AS all_c2b_cnt
   FROM
     ( SELECT substr(createtime,1,10) AS dt1, cclid,leadssources,cid,fullname,mobile
      FROM bdm.bdm_usedcar_carcaluationleads
      WHERE dt BETWEEN '${job_date1}' AND '${job_date2}'
        AND returntype NOT IN (1,2)
        AND substr(createtime,1,10) BETWEEN '${job_date1}' AND '${job_date2}' ) a
   JOIN
     (SELECT cityid,cityname
      FROM dim.dim_areacity
      WHERE dt='${job_date2}'
        AND cityid IN (110100,310100,500100,440300,420100,430100,320400,320100,320200,610100,
                       370200,410100,320500,330100,510100,440100,441900,320600,360100,320300,
                       130100,130600,130900,130200,130400,210200,220100,230100,371300,330200,
                       330400,330700,331000,330300,350100,350200,350500,440600,450100,530100,
                       520100,340100,140100,120100,370100,210100,440400,442000,441300,360700,
                       410300,330600) ) d ON a.cid=d.cityid
   GROUP BY dt1 ) p ON xleads.dt=p.dt1;