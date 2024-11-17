import regex
import secrets
import string

r1 = regex.compile('(?=^|[:punct:]|\s*)[A-Z]+(?=$|:punct:]|\s*)');
r2 = regex.compile('(?=^|[:punct:]|\s*)[a-z]+(?=$|:punct:]|\s*)');
r3 = regex.compile('(?=^|[:punct:]|\s*)[0-9]+(?=$|:punct:]|\s*)');

def my_mask(val:str) -> str:
    s=r1.sub(lambda m: ''.join(secrets.choice(string.ascii_uppercase) for i in range(len(m.group(0)))), val)
    s=r2.sub(lambda m: ''.join(secrets.choice(string.ascii_lowercase) for i in range(len(m.group(0)))), s)
    s=r3.sub(lambda m: ''.join(secrets.choice(string.digits) for i in range(len(m.group(0)))), s)
    return s

# print(my_mask("demohub@awesome.dev"))