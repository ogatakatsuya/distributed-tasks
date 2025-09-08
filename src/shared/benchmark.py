import time
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.shared.raytracer import RayTracer

# 画像設定（分散処理版と同じ設定）
IMAGE_WIDTH = 800
IMAGE_HEIGHT = 800
RAYS_PER_PIXEL = 64

def benchmark_raytracing():
    print("Starting single-threaded ray-tracing benchmark")
    print(f"Image size: {IMAGE_WIDTH}x{IMAGE_HEIGHT}, Rays per pixel: {RAYS_PER_PIXEL}")
    
    # レイトレーサーを初期化
    raytracer = RayTracer(width=IMAGE_WIDTH, height=IMAGE_HEIGHT)
    
    # 結果を格納するリスト
    results = []
    total_pixels = IMAGE_WIDTH * IMAGE_HEIGHT
    
    start_time = time.time()
    
    # 各ピクセルをレンダリング
    for row in range(IMAGE_HEIGHT):
        for col in range(IMAGE_WIDTH):
            pixel_start = time.time()
            
            # レイトレーシングを実行
            red, green, blue = raytracer.render_pixel(row, col, RAYS_PER_PIXEL)
            
            pixel_end = time.time()
            pixel_time = pixel_end - pixel_start
            
            # 結果を格納
            results.append({
                'row': row,
                'col': col,
                'red': red,
                'green': green,
                'blue': blue,
                'render_time': pixel_time
            })
            
            # 進捗表示
            processed = row * IMAGE_WIDTH + col + 1
            if processed % 1000 == 0:
                elapsed = time.time() - start_time
                pixels_per_second = processed / elapsed
                remaining_pixels = total_pixels - processed
                eta_seconds = remaining_pixels / pixels_per_second
                print(f"Progress: {processed}/{total_pixels} pixels ({processed/total_pixels*100:.1f}%), "
                      f"Speed: {pixels_per_second:.2f} pixels/sec, ETA: {eta_seconds/60:.1f} min")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # ベンチマーク結果を表示
    print("\n" + "="*60)
    print("BENCHMARK RESULTS")
    print("="*60)
    print(f"Total pixels rendered: {len(results)}")
    print(f"Total time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"Average time per pixel: {total_time/len(results)*1000:.2f} ms")
    print(f"Pixels per second: {len(results)/total_time:.2f}")
    print(f"Rays traced: {len(results) * RAYS_PER_PIXEL:,}")
    print(f"Rays per second: {len(results) * RAYS_PER_PIXEL / total_time:,.0f}")
    
    # 統計情報
    render_times = [r['render_time'] for r in results]
    min_time = min(render_times)
    max_time = max(render_times)
    avg_time = sum(render_times) / len(render_times)
    
    print("\nPer-pixel render time statistics:")
    print(f"  Min: {min_time*1000:.2f} ms")
    print(f"  Max: {max_time*1000:.2f} ms")
    print(f"  Average: {avg_time*1000:.2f} ms")
    
    return results, total_time

if __name__ == "__main__":
    try:
        results, total_time = benchmark_raytracing()
        print(f"\nBenchmark completed successfully in {total_time:.2f} seconds")
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
    except Exception as e:
        print(f"Error during benchmark: {e}")