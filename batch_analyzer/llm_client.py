#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
LLM API 客户端

提供统一的接口调用大模型 API (兼容 OpenAI 格式，如 Cherry Client / DeepSeek 等)。
"""

import json
import os
import re
import sys
import time
import requests
from typing import List, Dict, Any, Optional

class LLMClient:
    """LLM API 客户端，兼容 OpenAI 格式"""
    
    def __init__(self, config: Dict[str, Any] = None, auto_select_model: bool = True):
        """
        初始化 LLM 客户端
        
        Args:
            config: 配置字典，包含 api_key, base_url, model 等
            auto_select_model: 是否在初始化时自动获取可用模型列表
        """
        self.config = config or {}
        
        # 优先使用配置中的值，其次使用环境变量
        self.api_key = self.config.get("api_key") or os.environ.get("LLM_API_KEY") or "sk-any-key"
        self.base_url = self.config.get("base_url") or os.environ.get("LLM_BASE_URL") or "http://127.0.0.1:23333/v1"
        self.model = self.config.get("model") or os.environ.get("LLM_MODEL") or "gpt-4o"
        
        # 移除 base_url 结尾的斜杠
        if self.base_url.endswith("/"):
            self.base_url = self.base_url[:-1]
            
        # 可用模型列表 (用于故障切换)
        self.available_models: List[str] = []
        
        if auto_select_model:
            self._init_available_models()

    def _init_available_models(self):
        """获取并缓存可用模型列表"""
        try:
            models = self.list_models()
            self.available_models = [m["id"] for m in models]
            print(f"Available models: {self.available_models}")
            
            # 如果配置的模型不在列表中，自动选择第一个可用的
            if self.model not in self.available_models and self.available_models:
                print(f"Model '{self.model}' not available. Switching to '{self.available_models[0]}'")
                self.model = self.available_models[0]
        except Exception as e:
            print(f"Warning: Failed to fetch model list: {e}", file=sys.stderr)

    def list_models(self) -> List[Dict[str, Any]]:
        """获取可用模型列表"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/models"
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        result = response.json()
        return result.get("data", [])

    def repair_json(self, json_str: str) -> str:
        """
        修复常见的 JSON 格式错误
        
        Args:
            json_str: 可能格式不正确的 JSON 字符串
            
        Returns:
            修复后的 JSON 字符串
        """
        # 移除模型的思考标签 (如 MiniMax 的 <think>...</think>)
        json_str = re.sub(r'<think>[\s\S]*?</think>', '', json_str)
        
        # 移除 Markdown 代码块包裹
        if "```json" in json_str:
            match = re.search(r"```json\s*([\s\S]*?)\s*```", json_str)
            if match:
                json_str = match.group(1)
        elif "```" in json_str:
            match = re.search(r"```\s*([\s\S]*?)\s*```", json_str)
            if match:
                json_str = match.group(1)
        
        # 尝试提取第一个 { ... } 块
        match = re.search(r"\{[\s\S]*\}", json_str)
        if match:
            json_str = match.group(0)
            
        return json_str.strip()

    def chat(
        self, 
        messages: List[Dict[str, str]], 
        json_mode: bool = True,
        temperature: float = 0.1,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        发送聊天请求
        
        Args:
            messages: 消息列表 [{"role": "user", "content": "..."}, ...]
            json_mode: 是否强制 JSON 输出
            temperature: 温度参数
            max_retries: 最大重试次数
            
        Returns:
            API 响应的 content (解析后的 JSON 或 字符串)
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        url = f"{self.base_url}/chat/completions"
        
        # 构建要尝试的模型列表：当前模型优先，然后是其他可用模型
        models_to_try = [self.model]
        for m in self.available_models:
            if m != self.model and m not in models_to_try:
                models_to_try.append(m)
        
        last_error = None
        
        for model in models_to_try:
            payload = {
                "model": model,
                "messages": messages,
                "temperature": temperature
            }
            
            # 部分模型支持 response_format
            if json_mode:
                payload["response_format"] = {"type": "json_object"}
            
            for attempt in range(max_retries):
                try:
                    response = requests.post(url, headers=headers, json=payload, timeout=120)
                    response.raise_for_status()
                    
                    result = response.json()
                    content = result["choices"][0]["message"]["content"]
                    
                    # 调用成功，更新当前模型
                    if model != self.model:
                        print(f"Switched to model: {model}")
                        self.model = model
                    
                    if json_mode:
                        try:
                            return json.loads(content)
                        except json.JSONDecodeError:
                            # 尝试修复 JSON
                            repaired = self.repair_json(content)
                            return json.loads(repaired)
                    
                    return {"content": content}
                    
                except requests.exceptions.RequestException as e:
                    print(f"Model '{model}' request failed (attempt {attempt+1}/{max_retries}): {e}", file=sys.stderr)
                    last_error = e
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # 指数退避
                    # 如果所有重试都失败了，尝试下一个模型
                    
                except json.JSONDecodeError as e:
                    print(f"JSON decode failed: {e}", file=sys.stderr)
                    print(f"Raw content: {content[:500]}...", file=sys.stderr)
                    last_error = e
                    break  # JSON 解析错误不重试，直接尝试下一个模型
            
            # 当前模型所有重试失败，尝试下一个
            print(f"Model '{model}' failed, trying next...", file=sys.stderr)
        
        # 所有模型都失败了
        raise last_error or Exception("All models failed")

    def test_connection(self) -> bool:
        """测试 API 连接"""
        try:
            print(f"Testing connection to {self.base_url} with model {self.model}...")
            response = self.chat(
                messages=[{"role": "user", "content": "Hello, simply reply with JSON: {\"status\": \"ok\"}"}],
                json_mode=True,
                max_retries=1
            )
            print(f"Response: {response}")
            return True
        except Exception as e:
            print(f"Connection test failed: {e}", file=sys.stderr)
            return False


def load_config(config_path: str = None) -> Dict[str, Any]:
    """加载配置文件"""
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                full_config = json.load(f)
                return full_config.get("llm", {})
        except Exception as e:
            print(f"Warning: Failed to load config file: {e}", file=sys.stderr)
    return {}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Test LLM Client")
    parser.add_argument("--config", "-c", help="Config file path")
    parser.add_argument("--test", action="store_true", help="Test connection")
    args = parser.parse_args()
    
    config = load_config(args.config)
    client = LLMClient(config)
    
    if args.test:
        if client.test_connection():
            print("✅ Connection successful!")
        else:
            print("❌ Connection failed.")
            sys.exit(1)
