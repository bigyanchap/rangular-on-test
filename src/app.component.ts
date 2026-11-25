import { Component } from '@angular/core';

/**
 * One page, one job: the corner grid probe, copied exactly from
 * the lesson page of the tutorial branch unpinned-bar-test
 * (commit b5f018c5). Nine numbered discs at the top of the page
 * and a Tapped line under them. Nothing else is on this page:
 * no app bar, no menu, no other content. If disc 3 is dead here
 * too, the dead zone sits in the framework or in Expo itself.
 * If all nine answer here, the dead zone is tutorial specific.
 */
@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styles: [`
    .prow { flex-direction: row; justify-content: space-between; padding: 12px; }
    .disc { width: 40px; height: 40px; border-radius: 20px; background-color: #0b57d0; align-items: center; justify-content: center; }
    .disc-num { color: #ffffff; font-weight: 700; }
  `],
})
export class AppComponent {
  hit = '';

  mark(n: number) {
    this.hit = this.hit + ' ' + n;
  }
}
