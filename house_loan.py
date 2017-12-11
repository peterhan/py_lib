
def same_interest_loan(loan,interest_year,discount,loan_month):
    interest_year=interest_year*discount
    interest_month=interest_year/12
    month_pay=(loan * interest_month * (1+interest_month)**loan_month) / ((1+interest_month)**loan_month-1)
    print 'same_interest_loan:',discount,month_pay,month_pay*loan_month,loan
    
def same_basemoney_loan(loan,interest_year,discount,loan_month):
    interest_year=interest_year*discount
    interest_month=interest_year/12
    month_pay=loan/loan_month
    month_int_1st=loan*interest_month
    month_pay_1st=month_pay+month_int_1st    
    total_pay=0
    for i in xrange(loan_month):        
        month_int=(loan-month_pay*i)*(interest_month)
        month_pay_nth=month_pay+month_int
        total_pay+=month_pay_nth
        # print i,total_pay,month_pay_nth,month_int
    print 'same_basemoney_loan:',discount,month_pay_1st,total_pay,loan,total_pay-loan
    
    
loan=2800000
interest_year=0.049
discount=1.0
loan_month=2
# same_basemoney_loan(loan,interest_year,discount,loan_month)
for d in range(20):
    discount=0.7+d*0.01
    # same_interest_loan(loan,interest_year,discount,loan_month)
    same_basemoney_loan(loan,interest_year,discount,loan_month)