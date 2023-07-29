import os
import subprocess

def call_other_environment(script_path, *args):
    other_env_python = "/home/derek/anaconda3/envs/openie6/bin/python"
    result = subprocess.run([other_env_python, script_path, *args], capture_output=True, text=True)
    return result.stdout


os.chdir('openie-6')
#os.system(f'conda activate openie6')

#call_other_environment(run.py, )
os.system(f'/home/derek/anaconda3/envs/openie6/bin/python run.py --save models/oie_model --mode splitpredict --model_str bert-base-cased --task oie --gpus 0 --inp /home/derek/auto-nlp/auto-nlp-drive/text/sample_text.txt --out /home/derek/auto-nlp/auto-nlp-drive/text/predictions.txt')
