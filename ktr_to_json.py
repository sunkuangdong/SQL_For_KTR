import xml.etree.ElementTree as ET
import json

# 读取 .ktr 文件并解析 XML
def parse_ktr_to_json(ktr_file, json_file):
    try:
        # 解析XML
        tree = ET.parse(ktr_file)
        root = tree.getroot()

        # 初始化数据字典
        ktr_data = {
            "transformation_name": root.find("info/name").text if root.find("info/name") is not None else None,
            "description": root.find("info/description").text if root.find("info/description") is not None else None,
            "parameters": {},
            "log_table": [],
            "steps": []
        }

        # 解析日志表
        log_table = root.find("info/log")
        if log_table is not None:
            for table in log_table:
                table_name = table.tag.replace("-log-table", "")  # 移除 '-log-table' 后缀
                fields = []
                for field in table.findall("field"):
                    fields.append({
                        "id": field.find("id").text if field.find("id") is not None else None,
                        "name": field.find("name").text if field.find("name") is not None else None,
                        "enabled": field.find("enabled").text if field.find("enabled") is not None else "N"
                    })
                ktr_data["log_table"].append({"table": table_name, "fields": fields})

        # 解析步骤（Step）
        for step in root.findall("step"):
            step_data = {
                "name": step.find("name").text if step.find("name") is not None else None,
                "type": step.find("type").text if step.find("type") is not None else None,
                "description": step.find("description").text if step.find("description") is not None else None,
                "fields": []
            }

            # 提取 JSON 解析的字段信息
            fields = step.find("fields")
            if fields is not None:
                for field in fields.findall("field"):
                    step_data["fields"].append({
                        "name": field.find("name").text if field.find("name") is not None else None,
                        "path": field.find("path").text if field.find("path") is not None else None,
                        "type": field.find("type").text if field.find("type") is not None else None
                    })

            ktr_data["steps"].append(step_data)

        # 转换为 JSON 并保存
        with open(json_file, "w", encoding="utf-8") as json_out:
            json.dump(ktr_data, json_out, indent=4, ensure_ascii=False)

        print(f"转换成功！JSON 文件已保存为 {json_file}")

    except Exception as e:
        print(f"转换失败：{e}")

# 使用示例
ktr_file_path = "stage-json-to-table.ktr"  # 你的 .ktr 文件路径
json_output_path = "output.json"           # 目标 JSON 文件
parse_ktr_to_json(ktr_file_path, json_output_path)