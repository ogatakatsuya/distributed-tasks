import math
import random
from typing import Tuple
from dataclasses import dataclass


@dataclass
class Vec3:
    """3次元ベクトル"""
    x: float
    y: float
    z: float
    
    def __add__(self, other: 'Vec3') -> 'Vec3':
        return Vec3(self.x + other.x, self.y + other.y, self.z + other.z)
    
    def __sub__(self, other: 'Vec3') -> 'Vec3':
        return Vec3(self.x - other.x, self.y - other.y, self.z - other.z)
    
    def __mul__(self, scalar: float) -> 'Vec3':
        return Vec3(self.x * scalar, self.y * scalar, self.z * scalar)
    
    def dot(self, other: 'Vec3') -> float:
        return self.x * other.x + self.y * other.y + self.z * other.z
    
    def length(self) -> float:
        return math.sqrt(self.x * self.x + self.y * self.y + self.z * self.z)
    
    def normalize(self) -> 'Vec3':
        length = self.length()
        if length == 0:
            return Vec3(0, 0, 0)
        return Vec3(self.x / length, self.y / length, self.z / length)
    
    def reflect(self, normal: 'Vec3') -> 'Vec3':
        """法線に対する反射ベクトルを計算"""
        return self - normal * 2.0 * self.dot(normal)
    
    def refract(self, normal: 'Vec3', eta: float) -> 'Vec3':
        """スネルの法則による屈折ベクトルを計算"""
        cos_i = -self.dot(normal)
        sin_t2 = eta * eta * (1.0 - cos_i * cos_i)
        if sin_t2 >= 1.0:  # 全反射
            return Vec3(0, 0, 0)
        cos_t = math.sqrt(1.0 - sin_t2)
        return self * eta + normal * (eta * cos_i - cos_t)


@dataclass
class Ray:
    """レイ（光線）"""
    origin: Vec3
    direction: Vec3


@dataclass
class Material:
    """材質"""
    color: Vec3
    reflectivity: float = 0.0    # 反射率 (0.0-1.0)
    transparency: float = 0.0    # 透明度 (0.0-1.0)
    refractive_index: float = 1.0 # 屈折率
    roughness: float = 0.0       # 粗さ (0.0=鏡面, 1.0=完全拡散)


@dataclass
class Sphere:
    """球体"""
    center: Vec3
    radius: float
    material: Material


class RayTracer:
    """シンプルなレイトレーサー"""
    
    def __init__(self, width: int = 800, height: int = 800):
        self.width = width
        self.height = height
        self.camera_pos = Vec3(0, 0, 0)
        self.view_distance = 1.0
        
        # シンプルなシーン: 3つの球体（基本材質のみ）
        self.spheres = [
            # 赤い球（左）
            Sphere(Vec3(-1.2, 0, -3), 0.5, 
                   Material(Vec3(0.8, 0.2, 0.2), reflectivity=0.0, transparency=0.0)),
            # 緑の球（中央）
            Sphere(Vec3(0, 0, -3), 0.5, 
                   Material(Vec3(0.2, 0.8, 0.2), reflectivity=0.0, transparency=0.0)),
            # 青い球（右）
            Sphere(Vec3(1.2, 0, -3), 0.5, 
                   Material(Vec3(0.2, 0.2, 0.8), reflectivity=0.0, transparency=0.0)),
        ]
        
        # 環境光
        self.ambient_color = Vec3(0.1, 0.1, 0.1)
        # 光源
        self.light_pos = Vec3(2, 2, -1)
        self.light_color = Vec3(1.0, 1.0, 1.0)
    
    def ray_sphere_intersect(self, ray: Ray, sphere: Sphere) -> float:
        """レイと球体の交点を計算（距離を返す、交差しない場合は-1）"""
        oc = ray.origin - sphere.center
        a = ray.direction.dot(ray.direction)
        b = 2.0 * oc.dot(ray.direction)
        c = oc.dot(oc) - sphere.radius * sphere.radius
        
        discriminant = b * b - 4 * a * c
        if discriminant < 0:
            return -1.0
        
        # より近い交点を返す
        t1 = (-b - math.sqrt(discriminant)) / (2.0 * a)
        t2 = (-b + math.sqrt(discriminant)) / (2.0 * a)
        
        if t1 > 0:
            return t1
        elif t2 > 0:
            return t2
        else:
            return -1.0
    
    def trace_ray(self, ray: Ray) -> Vec3:
        """レイをトレースして色を計算（シンプル版）"""
        closest_t = float('inf')
        hit_sphere = None
        
        # 最も近い球体を見つける
        for sphere in self.spheres:
            t = self.ray_sphere_intersect(ray, sphere)
            if t > 0 and t < closest_t:
                closest_t = t
                hit_sphere = sphere
        
        if hit_sphere is None:
            # 背景色（空の色）
            return Vec3(0.2, 0.7, 1.0)
        
        # 交点を計算
        hit_point = ray.origin + ray.direction * closest_t
        # 法線を計算
        normal = (hit_point - hit_sphere.center).normalize()
        
        # 光源への方向
        light_dir = (self.light_pos - hit_point).normalize()
        
        # 簡単なランバート拡散照明
        diffuse = max(0, normal.dot(light_dir))
        
        # 最終的な色を計算
        color = self.ambient_color + hit_sphere.material.color * diffuse
        
        # 色の範囲を[0,1]にクランプ
        return Vec3(
            min(1.0, max(0.0, color.x)),
            min(1.0, max(0.0, color.y)),
            min(1.0, max(0.0, color.z))
        )
    
    def render_pixel(self, row: int, col: int, samples: int) -> Tuple[float, float, float]:
        """指定されたピクセルをレンダリング（アンチエイリアシング付き）"""
        color_sum = Vec3(0, 0, 0)
        
        for _ in range(samples):
            # アンチエイリアシングのためのジッタリング
            u = (col + random.random()) / self.width
            v = (row + random.random()) / self.height
            
            # スクリーン座標をワールド座標に変換
            aspect_ratio = self.width / self.height
            fov_scale = 1.0
            x = (u - 0.5) * 2.0 * fov_scale * aspect_ratio
            y = (0.5 - v) * 2.0 * fov_scale
            z = -self.view_distance
            
            # レイを生成
            ray_dir = Vec3(x, y, z).normalize()
            ray = Ray(self.camera_pos, ray_dir)
            
            # レイをトレース
            pixel_color = self.trace_ray(ray)
            color_sum = color_sum + pixel_color
        
        # 平均を取る
        avg_color = color_sum * (1.0 / samples)
        return (avg_color.x, avg_color.y, avg_color.z)