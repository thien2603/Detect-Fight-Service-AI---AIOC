"""Đánh giá model thật trên video có nhãn, hoàn toàn offline, không MQTT/event.

Luồng: đọc manifest → kiểm hash → decode từng video → lấy mẫu 8FPS/8frame,
stride4 → YOLO và VD-MIL → ghi điểm/nhãn dự đoán → confusion matrix và metrics.
Hai phép đánh giá tách riêng: điểm clip vượt ngưỡng; luật cảnh báo đủ1s và≥2người.
Mỗi clip độc lập, gate mới bắt đầu inactive; không áp cooldown giữa các clip.
"""
import argparse
from collections import deque
import csv
import hashlib
import json
from pathlib import Path
import time
import cv2
import torch
from run import ROOT, FightGate, MovinetClassifier, YOLO


def metrics(rows, prediction):
    tp = sum(r['label'] == 1 and r[prediction] == 1 for r in rows)
    fp = sum(r['label'] == 0 and r[prediction] == 1 for r in rows)
    tn = sum(r['label'] == 0 and r[prediction] == 0 for r in rows)
    fn = sum(r['label'] == 1 and r[prediction] == 0 for r in rows)
    precision = tp/(tp+fp) if tp+fp else 0.
    recall = tp/(tp+fn) if tp+fn else 0.
    return {'n': len(rows), 'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn,
            'accuracy': (tp+tn)/len(rows) if rows else 0., 'precision': precision,
            'recall': recall, 'f1': 2*precision*recall/(precision+recall) if precision+recall else 0.}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, default=ROOT/'data/eval_rwf2000')
    parser.add_argument('--output', type=Path, default=ROOT/'output/evaluation')
    parser.add_argument('--threshold', type=float, default=.9)
    parser.add_argument('--limit', type=int, default=0)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((args.dataset/'manifest.json').read_text())
    samples = manifest['samples'][:args.limit or None]
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    torch.set_num_threads(4)
    detector = YOLO(str(ROOT/'weights/yolo11n.pt'))
    classifier = MovinetClassifier(str(ROOT/'weights'), [str(ROOT/'weights/model_20.pt')], device=device)
    for net in classifier.classifier:
        net.to(device)
    rows, failures = [], []
    started = time.monotonic()
    with (args.output/'window_scores.jsonl').open('w') as score_file:
        with torch.inference_mode():
            for index, sample in enumerate(samples):
                path = args.dataset/sample['relative_path']
                if hashlib.sha256(path.read_bytes()).hexdigest() != sample['sha256']:
                    raise ValueError(f'Hash mismatch: {sample["relative_path"]}')
                cap = cv2.VideoCapture(str(path))
                fps = cap.get(cv2.CAP_PROP_FPS)
                expected_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                if not cap.isOpened() or fps <= 0:
                    cap.release()
                    failures.append({'sample': sample['relative_path'], 'reason': 'cannot_open'})
                    continue
                clip = deque(maxlen=8)
                gate = FightGate(args.threshold, confirm_seconds=1., reset_seconds=2., cooldown=0., active=False)
                frame_index = sampled = windows = people_peak = 0
                next_sample = 0.
                scores, events = [], []
                try:
                    while True:
                        ok, frame = cap.read()
                        if not ok:
                            break
                        stamp = frame_index/fps
                        frame_index += 1
                        if stamp + 1e-8 < next_sample:
                            continue
                        next_sample += 1/8
                        sampled += 1
                        clip.append(cv2.resize(frame,(172,172),interpolation=cv2.INTER_AREA))
                        if len(clip) < 8 or (sampled-8) % 4:
                            continue
                        score = float(classifier([[f for f in clip]])[0,0])
                        detected = detector.predict(frame, classes=[0], conf=.35, imgsz=640,
                                                    device=0 if device=='cuda' else 'cpu', verbose=False,
                                                    save=False, save_txt=False, save_crop=False)[0]
                        people = len(detected.boxes)
                        people_peak = max(people_peak, people)
                        scores.append(score)
                        windows += 1
                        if gate.update(score, stamp, 10000+stamp, people):
                            events.append(stamp)
                        score_file.write(json.dumps({'sample':sample['relative_path'],'label':sample['label'],
                                                     'time':stamp,'score':score,'people':people})+'\n')
                finally:
                    cap.release()
                if not scores or (expected_frames > 0 and frame_index < expected_frames*.95):
                    failures.append({'sample':sample['relative_path'],'reason':'incomplete_decode_or_no_windows',
                                     'decoded':frame_index,'expected':expected_frames})
                    continue
                row = {'sample':sample['relative_path'],'label':sample['label'],
                       'classifier_pred':int(max(scores)>=args.threshold),'event_rule_pred':int(bool(events)),
                       'max_score':max(scores),'mean_score':sum(scores)/len(scores),'windows':windows,
                       'duration':frame_index/fps,'fps':fps,'people_peak':people_peak,
                       'first_event_seconds':events[0] if events else None}
                rows.append(row)
                if (index+1) % 10 == 0 or index+1 == len(samples):
                    print(json.dumps({'progress':index+1,'total':len(samples),'seconds':round(time.monotonic()-started,1),
                                      'classifier':metrics(rows,'classifier_pred'),'event_rule':metrics(rows,'event_rule_pred')}),flush=True)
                # Checkpoint từng clip để không mất kết quả khi bị ngắt.
                (args.output/'predictions.json').write_text(json.dumps(rows,indent=2))
    if rows:
        with (args.output/'predictions.csv').open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]))
            writer.writeheader();writer.writerows(rows)
    result={'dataset_source':manifest['source'],'dataset_revision':manifest['revision'],'split':manifest['split'],
            'labels':manifest['label_source'],'threshold':args.threshold,'sample_fps':8,'clip_frames':8,'stride':4,
            'device':device,'elapsed_seconds':time.monotonic()-started,'expected_samples':len(samples),
            'failures':failures,'classifier_clip_max':metrics(rows,'classifier_pred'),
            'event_rule_1s_two_people':metrics(rows,'event_rule_pred'),
            'weights_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'weights').iterdir() if p.is_file()},
            'scope':'All supplied mirror val clips; not camera production accuracy or independently audited training disjointness.'}
    (args.output/'metrics.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2),flush=True)


if __name__ == '__main__':
    main()
