import os 
import sys 
import subprocess 
os.environ["RAILWAY_DISABLE_ASGI"] = "1" 
if __name__ == "__main__": 
    subprocess.run([sys.executable, "trading_bot.py", "--all"]) 
