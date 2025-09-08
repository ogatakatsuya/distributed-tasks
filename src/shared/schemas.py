from dataclasses import dataclass
import json


@dataclass
class PixelPosition:
    """Producer -> Consumer に送信するピクセルタスク"""
    count: int      # ピクセルごとのレイの数
    row: int        # レンダリング対象のピクセルのy座標  
    col: int        # レンダリング対象のピクセルのx座標
    
    def to_json(self) -> str:
        """JSON文字列に変換"""
        return json.dumps({
            'count': self.count,
            'row': self.row,
            'col': self.col
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> 'PixelPosition':
        """JSON文字列から復元"""
        data = json.loads(json_str)
        return cls(
            count=data['count'],
            row=data['row'],
            col=data['col']
        )


@dataclass
class PixelPositionResult:
    """Consumer -> Aggregator に送信するピクセル結果"""
    count: int      # ピクセルごとのレイの数
    row: int        # レンダリング対象のピクセルの座標
    col: int
    red: float      # レンダリング結果の合計
    green: float
    blue: float
    
    def to_json(self) -> str:
        """JSON文字列に変換"""
        return json.dumps({
            'count': self.count,
            'row': self.row,
            'col': self.col,
            'red': self.red,
            'green': self.green,
            'blue': self.blue
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> 'PixelPositionResult':
        """JSON文字列から復元"""
        data = json.loads(json_str)
        return cls(
            count=data['count'],
            row=data['row'],
            col=data['col'],
            red=data['red'],
            green=data['green'],
            blue=data['blue']
        )