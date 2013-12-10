##合并RDB文件脚本
#coding=utf-8
import sys,os
def slave_head(filename):
    f=open(filename,'rb')
    ol_string=f.read()[11:].encode('hex')
    f.close()
    f1=open(filename,'wb')
    new_string="fe01"+ol_string
    f1.write(new_string.decode('hex'))
    f1.close()

def master_tail(filename):
    f=open(filename,'rb')
    new_string=f.read()[:-2].encode('hex')
    f.close()
    f1=open(filename,'wb')
    f1.write(new_string.decode('hex'))
    f1.close()

if __name__=='__main__': 
    if len(sys.argv)<3:
        print "python binary_change.py master_tail slave_head"
        sys.exit()
    print "go to change......"
    master_tail_file=sys.argv[1]
    slave_head_file=sys.argv[2]
    if os.path.exists(slave_head_file) and os.path.exists(master_tail_file):
        master_tail(master_tail_file)    
        slave_head(slave_head_file)
    else:
        print "master_tail or slave_head not exit!!!"
    print "chane success!!"
