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
    
    def __rmul__(self, scalar: float) -> 'Vec3':
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
        
        # シーン: 中央に大きなガラス球、周りに複数の小さな球
        self.spheres = [
            # 中央の大きなガラス球
            Sphere(Vec3(0, 0, -3), 0.8, 
                   Material(Vec3(0.95, 0.98, 1.0), reflectivity=0.0, transparency=0.95, refractive_index=1.52)),
            
            # 周りの小さな球たち
            # 赤い球（左）
            Sphere(Vec3(-2.2, 0, -3), 0.4, 
                   Material(Vec3(0.8, 0.2, 0.2), reflectivity=0.0, transparency=0.0)),
            # 緑の球（右）
            Sphere(Vec3(2.2, 0, -3), 0.4, 
                   Material(Vec3(0.2, 0.8, 0.2), reflectivity=0.0, transparency=0.0)),
            # 青い球（上）
            Sphere(Vec3(0, 1.5, -3), 0.4, 
                   Material(Vec3(0.2, 0.2, 0.8), reflectivity=0.0, transparency=0.0)),
            # 黄色の球（下）
            Sphere(Vec3(0, -1.5, -3), 0.4, 
                   Material(Vec3(0.8, 0.8, 0.2), reflectivity=0.0, transparency=0.0)),
            # マゼンタの球（左上）
            Sphere(Vec3(-1.8, 1.2, -3), 0.35, 
                   Material(Vec3(0.8, 0.2, 0.8), reflectivity=0.0, transparency=0.0)),
            # シアンの球（右下）
            Sphere(Vec3(1.8, -1.2, -3), 0.35, 
                   Material(Vec3(0.2, 0.8, 0.8), reflectivity=0.0, transparency=0.0)),
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
    
    def trace_ray(self, ray: Ray, depth: int = 0, max_depth: int = 10) -> Vec3:
        """レイをトレースして色を計算（反射・屈折対応版）"""
        if depth >= max_depth:
            return Vec3(0, 0, 0)
            
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
        
        # 基本的な拡散照明
        diffuse = max(0, normal.dot(light_dir))
        base_color = self.ambient_color + hit_sphere.material.color * diffuse
        
        material = hit_sphere.material
        
        # ガラスのような透明材質の場合
        if material.transparency > 0.5:  # 高透明度材質
            # 反射成分を計算
            reflected_dir = ray.direction.reflect(normal)
            reflected_ray = Ray(hit_point + normal * 0.001, reflected_dir)
            reflected_color = self.trace_ray(reflected_ray, depth + 1, max_depth)
            
            # 透過色（背景色）
            transmitted_color = Vec3(0.2, 0.7, 1.0)  # 背景色
            
            # 強いフレネル効果
            cos_theta = abs(ray.direction.dot(normal))
            fresnel_factor = 0.25 + 0.75 * ((1.0 - cos_theta) ** 2)
            
            # 光沢効果（スペキュラーハイライト）
            light_dir = (self.light_pos - hit_point).normalize()
            view_dir = (ray.origin - hit_point).normalize()
            half_vector = (light_dir + view_dir).normalize()
            specular_intensity = max(0, normal.dot(half_vector)) ** 64  # 鋭い光沢
            specular_highlight = Vec3(1.0, 1.0, 1.0) * specular_intensity * 0.8
            
            # 拡散照明
            diffuse = max(0, normal.dot(light_dir)) * 0.05
            glass_base = Vec3(0.95, 0.98, 1.0) * diffuse
            
            # 環境反射を強化
            environment_reflection = reflected_color * 1.2
            
            # 全ての成分を合成
            final_color = (environment_reflection * fresnel_factor + 
                          transmitted_color * (1.0 - fresnel_factor) * 0.7 +
                          specular_highlight +
                          glass_base)
            
            # 色の範囲を調整
            final_color = Vec3(
                min(1.0, final_color.x),
                min(1.0, final_color.y),
                min(1.0, final_color.z)
            )
            
        else:
            # 通常の材質の場合
            # 拡散色の貢献度を計算
            diffuse_contribution = 1.0 - material.reflectivity - material.transparency
            final_color = base_color * diffuse_contribution
            
            # 反射の処理
            if material.reflectivity > 0:
                reflected_dir = ray.direction.reflect(normal)
                reflected_ray = Ray(hit_point + normal * 0.001, reflected_dir)
                reflected_color = self.trace_ray(reflected_ray, depth + 1, max_depth)
                final_color = final_color + reflected_color * material.reflectivity
            
            # 屈折（透明度）の処理
            if material.transparency > 0:
                entering = ray.direction.dot(normal) < 0
                eta = 1.0 / material.refractive_index if entering else material.refractive_index
                n = normal if entering else normal * -1.0
                
                refracted_dir = ray.direction.refract(n, eta)
                if refracted_dir.length() > 0:
                    offset = n * -0.001 if entering else n * 0.001
                    refracted_ray = Ray(hit_point + offset, refracted_dir)
                    refracted_color = self.trace_ray(refracted_ray, depth + 1, max_depth)
                    final_color = final_color + refracted_color * material.transparency
                else:
                    reflected_dir = ray.direction.reflect(normal)
                    reflected_ray = Ray(hit_point + normal * 0.001, reflected_dir)
                    reflected_color = self.trace_ray(reflected_ray, depth + 1, max_depth)
                    final_color = final_color + reflected_color * material.transparency
        
        # 色の範囲を[0,1]にクランプ
        return Vec3(
            min(1.0, max(0.0, final_color.x)),
            min(1.0, max(0.0, final_color.y)),
            min(1.0, max(0.0, final_color.z))
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