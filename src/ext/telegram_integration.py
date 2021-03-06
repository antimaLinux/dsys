# from __future__ import absolute_import, print_function, division, unicode_literals
import telepot
import time
HTTP_API_TOKEN = ''
BOT_USERNAME = ''
BOT_NAME = ''
ALLOWED_USERS_ID = ()


def on(_int):
    message = "ON : {}".format(_int)
    print(message)
    return message


def off(_int):
    message = "OFF : {}".format(_int)
    print(message)
    return message


def handler(_bot):

    def _handle(msg):
        if msg['from']['id'] in ALLOWED_USERS_ID:
            chat_id = msg['chat']['id']
            command = msg['text']

            print('Got command: %s' % command)

            if command == 'on':
                on(11)
                _bot.sendMessage(chat_id, on(11))
            elif command == 'off':
                _bot.sendMessage(chat_id, off(11))

    return _handle

if __name__ == '__main__':
    bot = telepot.Bot(HTTP_API_TOKEN)
    print(bot)
    # bot.setWebhook(url=None)
    func = handler(bot)
    bot.message_loop(func)
    print('I am listening...')

    while 1:
        time.sleep(10)

