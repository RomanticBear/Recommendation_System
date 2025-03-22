# -*- coding: utf-8 -*-

from groq import Groq

client = Groq(api_key="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")


prompt = """
어떤 책에 대한 서평을 읽고, 이 서평만 참고해서 내가 제공한 태그 목록안에서서 이 책과 어울리는 태그를 5개 선택해줘. 설명없이 태그만 선택해줘.
"""

review = """
## 서평
무라카미 하루키를 만나기 위해 가장 먼저 읽어야 할 책
무라카미 하루키의 대표작 『노르웨이의 숲』. 1987년 발표된 후 세계적인 ‘하루키 붐’을 일으키며 저자의 문학적 성과를 널리 알린 현대 일본 문학의 대표작이다. 원문에 충실하면서도 현대적인 언어로 새롭게 번역한 이 책은 첫 만남을 추억하는 독자와 새로운 만남을 기다리는 독자 모두에게 잊지 못할 기억을 전해준다.

독일 함부르크 공항에 막 착륙한 비행기 안에서 울린 비틀스의 《노르웨이의 숲》을 듣고 와타나베는 자신에게 간절한 부탁을 남긴 여자와 그 부탁을 떠올린다. 고등학교 시절 친한 친구 기즈키, 그의 여자 친구 나오코와 언제나 함께였던 와타나베. 그러나 기즈키의 갑작스러운 자살로 행복한 시간은 끝나 버리고 만다. 도쿄의 사립대학에 진학해 고향을 떠나온 와나타베는 나오코와 한동안 연락을 끊고 지내던 어느 날, 자신이 요양원에 들어가 있다는 나오코의 편지를 받게 되는데…….
"""

tag_list = """
## 태그 목록
판타지
SF
미스터리
스릴러
공포
로맨스
역사소설
모험
코미디
드라마
감동적인
잔잔한
긴장감 있는
우울한
유머러스한
어두운
밝고 긍정적
철학적
역동적인
몽환적인
역설적인
잔혹한
황홀한
불안감을 조성하는
사회 비판적
불평등
젠더 문제
전쟁과 평화
운명과 자유의지
도덕적 딜레마
혁명과 저항
환경과 생태
의료 및 바이오테크
외교와 정치 음모
권력과 부패
노동과 계급투쟁
이민과 정체성
문화 충돌
무정부주의
전통과 혁신
인공지능과 정보화
성장 이야기
트라우마와 치유
우정과 동료애
종교적·영적 요소
복수와 정의
정체성 탐색
내면 탐구
기억과 시간
이중성
금기와 반항
생명과 죽음
욕망과 타락
재난·서바이벌
의식의 흐름
미완의 이야기
실험적 문체
형식 파괴적
독특한 서술 방식
디스토피아
유토피아
포스트 아포칼립스
사이버펑크
다중 우주
시간 여행
초현실적 공간
미지의 영역 탐험
이세계
신화적 세계
가상 현실
기후 변화 세계
"""

final_prompt = prompt + review + tag_list

chat_completion = client.chat.completions.create(
    #
    # Required parameters
    #
    messages=[
        # Set an optional system message. This sets the behavior of the
        # assistant and can be used to provide specific instructions for
        # how it should behave throughout the conversation.
        {
            "role": "system",
            "content": "당신은 문학 전문가입니다. 글에서 핵심 내용을 파악하고 요약할 수 있습니다. 답변은 한국어로만 합니다."
        },
        # Set a user message for the assistant to respond to.
        {
            "role": "user",
            "content": final_prompt,
        }
    ],

    # The language model which will generate the completion.
    model="deepseek-r1-distill-llama-70b",

    #
    # Optional parameters
    #

    # Controls randomness: lowering results in less random completions.
    # As the temperature approaches zero, the model will become deterministic
    # and repetitive.

    # 모델의 **출력 다양성(랜덤성)**을 조절하는 파라미터.
    # 값이 높으면 창의적인 답변을 생성하고, 값이 낮으면 더 예측 가능한 답변을 생성함.
    temperature=0.2,

    # The maximum number of tokens to generate. Requests can use up to
    # 32,768 tokens shared between prompt and completion.
    
    # AI가 생성할 수 있는 최대 단어 수를 제한하는 파라미터
    max_completion_tokens=1024,

    # Controls diversity via nucleus sampling: 0.5 means half of all
    # likelihood-weighted options are considered.
    top_p=1,

    # A stop sequence is a predefined or user-specified text string that
    # signals an AI to stop generating content, ensuring its responses
    # remain focused and concise. Examples include punctuation marks and
    # markers like "[end]".
    stop=None,

    # If set, partial message deltas will be sent.
    stream=False,
)

# Print the completion returned by the LLM.
print(chat_completion.choices[0].message.content)