/**
 * Окно последних eventId для дедупа на клиенте. Kafka → gateway — at-least-once: после рестарта
 * gateway перечитывает события после последнего закоммиченного offset'а, и браузер, который их
 * уже видел, получил бы их второй раз (счётчики завышены).
 *
 * Окно ограничено (FIFO): помнить все id бесконечно — утечка памяти во вкладке, открытой сутками.
 * Дубль старше окна пройдёт — компромисс: повторы приходят «пачкой» сразу после рестарта, т.е.
 * близко к оригиналу; окна в несколько тысяч событий хватает с запасом.
 */
export class RecentIds {
  private readonly ids = new Set<string>();

  constructor(private readonly capacity: number) {}

  /** true — id новый (запомнили); false — уже видели в окне. */
  add(id: string): boolean {
    if (this.ids.has(id)) return false;
    this.ids.add(id);
    // Set хранит порядок вставки: первый элемент — самый старый.
    if (this.ids.size > this.capacity) this.ids.delete(this.ids.values().next().value as string);
    return true;
  }

  get size(): number {
    return this.ids.size;
  }
}
