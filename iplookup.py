def ip2int(ip):
    ip_nm_list=ip.strip().split(".")
    intip = int(ip_nm_list[0]) * 16777216 + \
            int(ip_nm_list[1]) * 65536 + \
            int(ip_nm_list[2]) * 256 + \
            int(ip_nm_list[3])
    return intip
    
def int2ip(ipNum):
	ret = "";
	ipNum = int(ipNum)
	count = 0
	while (count<4):
		yushu = ipNum%256
		ipNum = ipNum/256
		if ret == '':
			ret = '%d' %yushu
		else:
			ret = ret+"."+'%d' %yushu
		count = count + 1
	return ret
    
class IpLookup:
    def __init__(self,fname="colombo_iplib.txt"):        
        for i,line in enumerator(open(fname)):
            line = line.decode('utf-8').encode('gbk').strip()
            if line == '':
                continue
            row = line.split("|")
            if len(row) < 12:
                continue
            if line.find("#")==0:
                continue
            self.low_list[i] = ip2int(row[1])
            self.high_list[i] = ip2int(row[1])
            self.prov_list[i] = row[4].strip()
            self.city_list[i] = row[5].strip()
            
    def get_ip_info(self,ip_str):
        return self.binary_search(ip_str,0,len(self.low_list) - 1)
    
    def binary_search(self,ip_str,low_bound,high_bound):
        ip_info = dict()
        ip_info['prov'] = 'null'
        ip_info['city'] = 'null'        
        try:
            ip_no = ip2int(ip_str)
        except:
            return ip_info
        #print ip_no
        if low_bound == high_bound:
            if (ip_no >= low_list[low_bound] and ip_no <= high_list[high_bound]):
                ip_info['prov'] = self.prov_list[low_bound]
                ip_info['city'] = self.city_list[low_bound]
            return ip_info
        mid = 0
        sum = low_bound + high_bound
        if sum%2 == 0:
            mid = sum/2
        else:
            mid = (sum + 1)/2
        if ip_no == low_list[mid]:
            ip_info['prov'] = self.prov_list[mid]
            ip_info['city'] = self.city_list[mid]
            return ip_info
        elif ip_no < low_list[mid]:
            return binary_search(ip_str, low_bound, mid - 1)
        else:
            return binary_search(ip_str, mid, high_bound)