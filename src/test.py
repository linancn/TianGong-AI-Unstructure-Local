import json

def check_none_type(schema):
    if isinstance(schema, dict):
        for k, v in schema.items():
            if k in ["type", "format", "pattern"] and v is None:
                print(f"Error: {k} is None at {schema}")
            check_none_type(v)
    elif isinstance(schema, list):
        for v in schema:
            check_none_type(v)

# 读取并校验 process_schema4_full.json 文件
if __name__ == "__main__":
    try:
        with open('/root/projects/TianGong-AI-Unstructure-Local/process_schema4_full.json', 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        print("开始校验 process_schema4_full.json 文件...")
        check_none_type(schema_data)
        print("校验完成！")
        
    except FileNotFoundError:
        print("错误：找不到 process_schema4_full.json 文件")
    except json.JSONDecodeError as e:
        print(f"错误：JSON 文件格式有误 - {e}")
    except Exception as e:
        print(f"错误：{e}")

