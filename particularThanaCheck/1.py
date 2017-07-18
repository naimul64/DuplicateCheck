import pygame

pygame.mixer.pre_init(44100, -16, 2, 2048)
pygame.init()
screen = pygame.display.set_mode((640, 480))

def playNotificationSound():
    pygame.mixer.music.load("notification.mp3")
    pygame.mixer.music.play()

playNotificationSound()