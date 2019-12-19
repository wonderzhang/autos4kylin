import  base64

def pwdkey(v_user_pwd):
    s1 = base64.b64encode(bytes(v_user_pwd, 'utf-8'))
    s1 = str(s1, 'utf-8')
    return s1




#s1 = base64.b64encode(bytes('admin:KYLIN', 'utf-8'))
# #s2 = base64.b64decode(bytes('$2a$10$MCY65andOnz5o.0QiVaJlOfIkE9P3Yo0s.87h6Cz4FxlCeaRFeUQO', 'utf-8'))
# #s2 = base64.decodestring('YWRtaW46S1lMSU4')
# #print(s2)
# # print(s1,s2)
# print(s1)
#


if __name__ == '__main__':
    print(pwdkey('admin:KYLIN'))
    print(pwdkey('zhangwei09:Thankyou1981'))

