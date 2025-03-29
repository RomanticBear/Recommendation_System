# -*- coding: utf-8 -*-

from groq import Groq

client = Groq(api_key="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")


prompt = """
어떤 책에 대한 서평을 읽고, 이 서평만 참고해서 내가 제공한 태그 목록안에서서 이 책과 어울리는 태그를 5개 선택해줘. 설명없이 태그만 선택해줘.
"""

review = """
책 먹는 여우의 행복한 책읽기

책을 지극히도 좋아하는 여우 아저씨는 책을 다 읽은 후엔 소금과 후추를 뿌려 먹어치움으로써 교양에 대한 욕구뿐만 아니라 식욕도 해결했습니다. 하지만 책값이 좀 비쌉니까? 더구나 책을 먹을수록 식욕이 더욱 왕성해지니 가난한 여우 아저씨는 더 난감해질 수밖에요!

동네 서점을 서성거리던 여우 아저씨는 기가막힌 종이 향기가 나는 도서관을 발견하게 되고, 이 천국 같은 곳에서 신나게 양껏 책을 읽게(먹게?) 됩니다. 그러나 꼬리가 길면 잡히는 법, 사서에게 들킨 뒤 여우 아저씨는 도서관 출입 금지를 당하게 되지요.

광고지나 싸구려 신문지 때로는 폐지 수집함을 뒤지면서 연명을 하게 된 가련한 여우 아저씨는 급기야 영양실조로 그 윤기나던 털가죽은 빛이 바래고 소화불량을 겪게 되지요. 도서관에서 저지른 일 말고는 늘 점잖은 시민이었던 여우 아저씨는 견디다못해 동네 서점을 털게 되는데…. 일명 서점털이 강도!

강도짓을 한 게 들통나 감옥에 보내진 여우 아저씨는 '독서금지'라는 가옥한 처벌을 받게 됩니다. 절망의 나날을 보내던 그에게 떠오른 기발한 생각은 자기가 직접 글을 쓰는 것! 피와 살이 되었던 엄청난 독서량을 사용해 쓴 여우의 글은 감방을 지키던 교도관을 감동시키게 되고, 교도관은 출판사를 차려 여우를 소설가로 성공시키게 됩니다. 여우 아저씨의 뛰어난 작품은 온 세상의 주목을 받고 수많은 평론가의 연구 대상이 되지요.

백만장자가 된 여우 아저씨는 과연 원없이 책을 사 읽게(사 먹게) 되었을까요? 아니랍니다. 이젠 사정이 달라지네요. 여우가 가장 좋아하는 식사는 바로 자기자신이 쓴 책이었으니까요!

우화를 잊은 이들에게 선사하는 현대적인 감각의 새로운 우화
이 책은 '이솝 우화'를 우화의 전부로 아는 이들에게 선사하는 새로운 감각의 현대 우화입니다. 책을 쓰는 인간보다 더 책을 사랑하는 여우(물론 그가 책을 사랑하는 방식은 좀 여우적(?)이기는 합니다)를 통해, 책이 어떤 의미와 가치를 갖는가를 다소 희극적으로 재미있게 얘기해 주지요. 교훈에 대한 강박 관념을 벗어던진 이러한 우화쓰기는 얼핏 가벼워 보이기도 하지만 세상의 내면을 들여다보는 듯 오히려 더 자연스럽고 진실해 보이기도 합니다.

강렬한 대조가 돋보이는 자유로운 색쓰기과 화면 구성, 그림과 글의 절묘한 배합을 통해 보여주는 풍부한 환상의 세계는 우화의 맛을 한층 살려 줍니다.

주목받는 독일 신예 작가의 귀여운(?) 음산함이 묻어나는 풍자
도서관에서 짐승의 냄새를 풍기며 책을 훔쳐 먹는 여우의 음산한 행동들은 무섭기는커녕 읽는 이들의 입가에 웃음을 머금게 합니다. 그 모습이 어릴 적, 가지고 싶은 뭔가에 맹목적으로 열중하던 순수했던 욕망을 생각나게 해서일까요?

대문호로 성공한 여우 아저씨의 독서 비법은 소금과 후추입니다. 음식에 양념을 치듯 자기의 관점과 생각을 덧뿌려 잘근잘근 씹어먹는 독서가 중요하다는 걸 의미하겠죠? 한편 당장의 배고픔을 못참고 영양가 없는 책들마저 마구잡이로 먹어대던 여우 아저씨가 험상궂은 몰골로 변해가는 광경은, 부주의하게 받아들인 쓰레기 같은 책들이 우리의 몸에 얼마나 해로울 수 있는가를 말해 줍니다.

하나의 작품은 독자들의 새로운 해석에 따라 재창조되기도 합니다. 이 책의 작가가 누누히 말했듯 독자 나름대로 소금과 후추를 뿌려 먹는 셈이지요.

책은 인생의 친구! 어른도 공감할 아이들의 언어로 전하는 뜻깊은 메시지

이 책은 우리의 여우 아저씨처럼 책과 혼연일체가 되는 삶을 꿈꾸는 이들을 위한 글입니다. 태어나면서 죽을 때까지 쉼없이 계속되는 책 사랑, 그런 사랑을 가졌던 이들이 있었기에 세상은 이만큼 발전해온 게 아닐까요?

책의 힘을 믿는 사람들이 벌이는 일 중에, 영국을 비롯한 여러 나라에서는 아이가 태어나면서부터 보건소에서 독서 지도를 시작한다고 합니다. 몸의 건강만큼이나 정신의 건강이 중요함을 상징하는 것이지요. 세계적으로 수준높은 활자 문화를 자랑했던 우리의 책 문화, 독서 문화의 현주소는 어디쯤일지 생각해 볼 일입니다.

맛갈스런 책에 쏟아지는 언론의 맛갈스런 찬사!

색과 형태의 강렬한 대조, 풍부한 환상을 보여주는 그림과 글의 상호작용, 마구 뒤죽박죽으로 또는 명료하게 나타나는 그림 속 글들의 유희적이면서도 균형 잡힌 교차는 모험하는 기분으로 이 책을 읽게 만든다.
-노이에 취리히 차이퉁

재치 있는 삽화와 함께 책이 삶에 절대적으로 중요하다는 메시지를 어른도 공감할 수 있는 아이들의 언어로 유쾌하고 자연스럽게 전달한다.
-프라우 운트 무터

글과 그림에서 모든 연령대의 독자가 즐길 수 있도록 후추와 함께 차려 놓은 책이다.
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
    temperature=0.5,

    # The maximum number of tokens to generate. Requests can use up to
    # 32,768 tokens shared between prompt and completion.
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




# 문제점 1 - 서평 데이터 없는데 태그 달아줌 -> 기본 프롬프트로 작동한듯 