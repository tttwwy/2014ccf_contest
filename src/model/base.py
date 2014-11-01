__author__ = 'WangZhe'
# coding=utf-8

class BaseModel():
    def __init__(self,model_name = ""):
        self.init_model()
        if not model_name:
            self.load_model(model_name)


        # total_num = 0
        # file_name1 = file_name + "1"
        # file_name2 = file_name + "2"
        # with open(file_name,'r') as f_read:
        # #     for line in f_read:
        # #         total_num += 1
        # #
        # # random_list = range(total_num)
        # #
        # # with open(file_name,'r') as f_read:
        # #     with open(file_name1,'w') as f_write1:
        # #         with open(file_name2,'w') as f_write2:
        # #             for line in f_read:



    def save_model(self,model_file_name):
        pass

    def load_model(self,model_file_name):
        pass


    def submit_file(self,predict_list,save_file_name):
        with open(save_file_name,'w') as f:
            for uid,predict in predict_list:
                if predict == '1':
                    f.write(uid + "\n")

