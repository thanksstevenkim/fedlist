#!/usr/bin/env python3
"""
스팸/악성 서버 필터링 스크립트

peer_suggestions.json에서 의심스러운 서버를 자동으로 걸러냅니다.
"""

import json
import re
import sys
from pathlib import Path
from typing import Tuple, List, Dict, Set
import argparse

# 알려진 스팸 TLD 목록
SPAM_TLDS = [
    '.tk', '.ml', '.ga', '.cf', '.gq',  # 무료 TLD
    '.click', '.loan', '.download', '.racing', '.review',  # 스팸에 자주 사용됨
]

# 의심스러운 키워드
SPAM_KEYWORDS = [
    'porn', 'xxx', 'adult', 'sex', 'casino', 'poker', 'betting',
    'pharma', 'viagra', 'cialis', 'pills', 'drugs',
    'crypto', 'bitcoin', 'investment', 'forex',
    'replica', 'fake', 'counterfeit',
]

# 차단할 도메인 패턴
SPAM_PATTERNS = [
    r'^[a-z0-9]{20,}\.',  # 20자 이상의 랜덤 문자열
    r'^\d+\.',  # 숫자로만 시작
    r'[0-9]{8,}',  # 8자리 이상의 연속 숫자
    r'(.)\1{5,}',  # 같은 문자 6번 이상 반복
]


def load_blocklist(blocklist_file: str = None) -> Set[str]:
    """외부 블랙리스트 파일 로드 (선택사항)"""
    if not blocklist_file or not Path(blocklist_file).exists():
        return set()
    
    try:
        with open(blocklist_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 리스트 형태 또는 도메인 키를 가진 딕셔너리 형태 모두 지원
            if isinstance(data, list):
                return set(data)
            elif isinstance(data, dict):
                return set(data.keys())
    except Exception as e:
        print(f"Warning: 블랙리스트 로드 실패: {e}", file=sys.stderr)
    
    return set()


def check_domain_pattern(host: str) -> Tuple[bool, str]:
    """도메인 패턴이 의심스러운지 확인"""
    
    # 스팸 TLD 확인
    for tld in SPAM_TLDS:
        if host.endswith(tld):
            return True, f"스팸 TLD: {tld}"
    
    # 의심스러운 키워드
    host_lower = host.lower()
    for keyword in SPAM_KEYWORDS:
        if keyword in host_lower:
            return True, f"의심스러운 키워드: {keyword}"
    
    # 패턴 매칭
    for pattern in SPAM_PATTERNS:
        if re.search(pattern, host, re.IGNORECASE):
            return True, f"의심스러운 패턴: {pattern}"
    
    return False, None


def check_stats_anomaly(instance: Dict) -> Tuple[bool, str]:
    """통계 이상 확인"""
    
    stats = instance.get('stats', {})
    
    # 통계 정보가 없으면 체크하지 않음 (단순 도메인 목록인 경우)
    if not stats:
        return False, None
    
    users = stats.get('total_users', 0)
    active_users = stats.get('active_users', 0)
    posts = stats.get('local_posts', 0)
    
    # 통계가 모두 0이거나 None인 경우도 체크하지 않음
    if users == 0 and posts == 0 and active_users == 0:
        return False, None
    
    # 비정상적으로 많은 게시물
    if users > 0 and posts > 0:
        posts_per_user = posts / users
        if posts_per_user > 50000:
            return True, f"비정상적인 게시물 비율 (사용자당 {posts_per_user:.0f}개)"
    
    # 활성 사용자가 전체 사용자보다 많음 (데이터 오류)
    if users > 0 and active_users > users * 1.5:
        return True, f"활성 사용자 수 이상 ({active_users} > {users})"
    
    return False, None


def is_spam_server(instance: Dict, blocklist: Set[str] = None) -> Tuple[bool, str]:
    """
    여러 휴리스틱으로 스팸 서버 판별
    
    Returns:
        (is_spam, reason) 튜플
    """
    
    host = instance.get('host', '')
    
    # 1. 외부 블랙리스트 확인
    if blocklist and host in blocklist:
        return True, "외부 블랙리스트에 등재됨"
    
    # 2. ActivityPub 검증 실패 (verified_activitypub 필드가 명시적으로 False인 경우만)
    # 필드가 없는 경우는 통과 (단순 도메인 목록일 수 있음)
    if 'verified_activitypub' in instance and not instance['verified_activitypub']:
        return True, "ActivityPub 검증 실패"
    
    # 3. 도메인 패턴 확인
    is_suspicious, reason = check_domain_pattern(host)
    if is_suspicious:
        return True, reason
    
    # 4. 통계 이상 확인
    has_anomaly, reason = check_stats_anomaly(instance)
    if has_anomaly:
        return True, reason
    
    return False, None


def filter_spam(
    input_file: str,
    output_file: str,
    log_file: str = None,
    blocklist_file: str = None,
    dry_run: bool = False
) -> Dict:
    """
    peer_suggestions.json을 필터링하여 스팸 서버 제거
    
    Args:
        input_file: 입력 파일 경로 (peer_suggestions.json)
        output_file: 출력 파일 경로 (filtered_peers.json)
        log_file: 필터링 로그 파일 경로 (선택)
        blocklist_file: 외부 블랙리스트 파일 경로 (선택)
        dry_run: True면 파일을 실제로 쓰지 않고 결과만 출력
    
    Returns:
        필터링 통계 딕셔너리
    """
    
    # 입력 파일 로드
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            peers = json.load(f)
    except FileNotFoundError:
        print(f"Error: 입력 파일을 찾을 수 없습니다: {input_file}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: JSON 파싱 실패: {e}", file=sys.stderr)
        sys.exit(1)
    
    # 블랙리스트 로드
    blocklist = load_blocklist(blocklist_file)
    if blocklist:
        print(f"외부 블랙리스트 로드: {len(blocklist)}개 도메인")
    
    # 필터링 수행
    filtered = []
    spam_log = []
    
    for peer in peers:
        # peer가 문자열인 경우와 딕셔너리인 경우 모두 처리
        if isinstance(peer, str):
            # 문자열이면 host만 있는 것으로 간주
            host = peer
            peer_dict = {
                'host': host,
                'stats': {}
            }
        else:
            # 딕셔너리면 그대로 사용
            host = peer.get('host', 'unknown')
            peer_dict = peer
        
        is_spam, reason = is_spam_server(peer_dict, blocklist)
        
        if is_spam:
            spam_log.append({
                'host': host,
                'reason': reason,
                'platform': peer_dict.get('platform') if isinstance(peer, dict) else None,
                'stats': peer_dict.get('stats', {}) if isinstance(peer, dict) else {}
            })
        else:
            filtered.append(peer)
    
    # 통계
    stats = {
        'total': len(peers),
        'passed': len(filtered),
        'filtered': len(spam_log),
        'filter_rate': len(spam_log) / len(peers) * 100 if peers else 0
    }
    
    # 결과 출력
    print(f"\n{'='*60}")
    print(f"필터링 결과:")
    print(f"  총 서버 수: {stats['total']}")
    print(f"  통과: {stats['passed']}")
    print(f"  필터링됨: {stats['filtered']} ({stats['filter_rate']:.1f}%)")
    print(f"{'='*60}\n")
    
    # 필터링 이유별 통계
    reason_counts = {}
    for entry in spam_log:
        reason = entry['reason']
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    
    if reason_counts:
        print("필터링 이유별 통계:")
        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            print(f"  - {reason}: {count}개")
        print()
    
    # 파일 저장 (dry_run이 아닐 때만)
    if not dry_run:
        # 필터링된 목록 저장
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)
        print(f"✓ 필터링된 목록 저장: {output_file}")
        
        # 로그 저장
        if log_file:
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'summary': stats,
                    'filtered_servers': spam_log
                }, f, ensure_ascii=False, indent=2)
            print(f"✓ 필터링 로그 저장: {log_file}")
    else:
        print("(dry-run 모드: 파일을 저장하지 않았습니다)")
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='페디버스 서버 목록에서 스팸/악성 서버를 필터링합니다.'
    )
    parser.add_argument(
        '--input',
        default='data/peer_suggestions.json',
        help='입력 파일 경로 (기본: data/peer_suggestions.json)'
    )
    parser.add_argument(
        '--output',
        default='data/filtered_peers.json',
        help='출력 파일 경로 (기본: data/filtered_peers.json)'
    )
    parser.add_argument(
        '--log',
        default='data/spam_filtered.log.json',
        help='필터링 로그 파일 경로 (기본: data/spam_filtered.log.json)'
    )
    parser.add_argument(
        '--blocklist',
        help='외부 블랙리스트 JSON 파일 경로 (선택)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='실제로 파일을 쓰지 않고 결과만 출력'
    )
    
    args = parser.parse_args()
    
    filter_spam(
        input_file=args.input,
        output_file=args.output,
        log_file=args.log,
        blocklist_file=args.blocklist,
        dry_run=args.dry_run
    )


if __name__ == '__main__':
    main()
