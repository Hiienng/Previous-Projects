import requests
from bs4 import BeautifulSoup

stock_symbol = "VPB"
stock_url = "http://s.cafef.vn/hose/VPB-ngan-hang-thuong-mai-co-phan-viet-nam-thinh-vuong.chn"
#stock_url = "http://s.cafef.vn/hose/=" + stock_symbol

response = requests.get(stock_url) 
